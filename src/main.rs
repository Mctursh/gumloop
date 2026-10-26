use core::pin::pin;
use std::collections::HashMap;
use std::future::Future;
use std::io::{self, ErrorKind, Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::task::{Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use mio::net::TcpStream;
use mio::{Events, Interest, Poll as MPoll, Registry, Token, Waker as MWaker};
// use tokio::spawn

struct ChewingGumCan {
    label: &'static str,
    pieces: u8,
}

impl ChewingGumCan {
    fn new(label: &'static str, pieces: u8) -> Result<Self, &'static str> {
        if pieces == 0 {
            Err("Cant create gum can with 0 pieces of gum in the can!")
        } else {
            Ok(Self { label, pieces })
        }
    }

    fn take_gum(&mut self) -> Result<String, &'static str> {
        if self.pieces == 0 {
            Err("No more gum in the can!")
        } else {
            self.pieces -= 1;
            Ok(format!("{} remaining gum", self.pieces))
        }
    }
}

impl Future for ChewingGumCan {
    type Output = Result<String, &'static str>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let thread = std::thread::current().id();
        let label = self.label;

        match self.take_gum() {
            Ok(msg) => {
                println!("[{label}] polled on {thread:?}, took a piece: {msg}");
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(_) => Poll::Ready(Ok(format!("{label} is empty"))),
        }
    }
}
struct Sleep {
    deadline: Instant,
    handle: Arc<Mutex<(Instant, Option<Waker>)>>,
}

impl Sleep {
    fn new(deadline: Instant, handle: Arc<Mutex<(Instant, Option<Waker>)>>) -> Self {
        // if duration.is_zero() {
        //     Err("Can't create a sleep feature with Zero duration")
        // } else {
        // }
        // let deadline = Instant::now() + duration;
        Self { deadline, handle }
    }

    fn has_passed_deadline(&self) -> bool {
        if Instant::now() >= self.deadline {
            // println!("Deadline Passed");
            true
        } else {
            // println!("Deadline Pending, Remaining {:?} seconds", self.deadline);
            false
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.has_passed_deadline() {
            Poll::Ready(())
        } else {
            // do something with &cx.waker
            let mut entry = self.handle.lock().unwrap();
            entry.1 = Some(cx.waker().clone());
            drop(entry);
            Poll::Pending
        }

        // todo!()
    }
}

struct ThreadWaker {
    // thread: Thread,
    waker: Arc<MWaker>,
    is_main_future: AtomicBool,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.is_main_future.store(true, Ordering::Relaxed);
        self.waker.wake().expect("Unable to wake");
        // self.thread.unpark();
    }
}

// A task will be it's own sender
struct Task {
    id: u64,
    future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
    tx: Sender<Arc<Task>>,
    // main_thread: Thread,
    waker: Arc<MWaker>,
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        let _ = self.tx.send(self.clone());
        self.waker.wake().expect("unable to wake");
    }
}

struct JoinHandle<T> {
    handle: Arc<Mutex<HandleResult<T>>>,
}

enum ResultState<T> {
    Running,
    Finished(T),
    Canceled,
}

struct CancelGuard<T> {
    slot: Arc<Mutex<HandleResult<T>>>,
}

impl<T> Drop for CancelGuard<T> {
    fn drop(&mut self) {
        let mut handle = self.slot.lock().unwrap();
        match handle.result {
            ResultState::Running => {
                handle.result = ResultState::Canceled;
                let waker = handle.waker.take();
                println!("[guard] cancelled");
                drop(handle);
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
            _ => {}
        }
    }
}

struct HandleResult<T> {
    result: ResultState<T>,
    waker: Option<Waker>,
}
#[derive(Clone)]
struct TimerHandle {
    timers: Arc<Mutex<Vec<Arc<Mutex<(Instant, Option<Waker>)>>>>>,
    io_slots: Arc<Mutex<HashMap<Token, IoSlots>>>,
    registry: Arc<Registry>,
    token_counter: Arc<AtomicUsize>,
}

struct AsyncTcpListener {
    inner: mio::net::TcpListener,
    token: Token,
    handle: TimerHandle,
}

impl Drop for AsyncTcpListener {
    fn drop(&mut self) {
        let _ = self.handle.registry.deregister(&mut self.inner);
        self.handle.io_slots.lock().unwrap().remove(&self.token);
    }
}

impl AsyncTcpListener {
    fn accept(&self) -> AcceptFut<'_> {
        AcceptFut {
            listener_handle: self,
        }
    }
}

struct AsyncTcpStream {
    inner: mio::net::TcpStream,
    token: Token,
    handle: TimerHandle,
}

impl Drop for AsyncTcpStream {
    fn drop(&mut self) {
        let _ = self.handle.registry.deregister(&mut self.inner);
        self.handle.io_slots.lock().unwrap().remove(&self.token);
    }
}

impl AsyncTcpStream {
    fn new(
        timer_handle: &TimerHandle,
        mut stream: mio::net::TcpStream,
    ) -> io::Result<Self> {
        let token = Token(timer_handle.token_counter.fetch_add(1, Relaxed));
        timer_handle.registry.register(
            &mut stream,
            token,
            Interest::READABLE | Interest::WRITABLE,
        )?;
        timer_handle.io_slots.lock().unwrap().insert(
            token,
            IoSlots {
                read: None,
                write: None,
            },
        );
        Ok(Self {
            inner: stream,
            handle: timer_handle.clone(),
            token,
        })
    }

    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> ReadFut<'a> {
        ReadFut { inner: self, buf }
    }

    fn write<'a>(&'a mut self, buf: &'a [u8]) -> WriteFut<'a> {
        WriteFut { inner: self, buf }
    }
}

struct AcceptFut<'a> {
    listener_handle: &'a AsyncTcpListener,
}

struct ReadFut<'a> {
    inner: &'a mut AsyncTcpStream,
    buf: &'a mut [u8],
}

impl<'a> Future for ReadFut<'a> {
    type Output = Result<usize, std::io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            return match this.inner.inner.read(this.buf) {
                Ok(n) => Poll::Ready(Ok(n)),
                Err(e) => {
                    if e.kind() == ErrorKind::WouldBlock {
                        if let Some(io_slot) = this
                            .inner
                            .handle
                            .io_slots
                            .lock()
                            .unwrap()
                            .get_mut(&this.inner.token)
                        {
                            io_slot.read = Some(cx.waker().clone());
                            Poll::Pending
                        } else {
                            return Poll::Ready(Err(std::io::Error::other("Slot not registered")));
                        }
                    } else if e.kind() == ErrorKind::Interrupted {
                        continue;
                    } else {
                        Poll::Ready(Err(e))
                    }
                }
            };
        }
    }
}

struct WriteFut<'a> {
    inner: &'a mut AsyncTcpStream,
    buf: &'a [u8],
}

impl<'a> Future for WriteFut<'a> {
    type Output = Result<usize, std::io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            return match this.inner.inner.write(this.buf) {
                Ok(n) => Poll::Ready(Ok(n)),
                Err(e) => {
                    if e.kind() == ErrorKind::WouldBlock {
                        if let Some(io_slot) = this
                            .inner
                            .handle
                            .io_slots
                            .lock()
                            .unwrap()
                            .get_mut(&this.inner.token)
                        {
                            io_slot.write = Some(cx.waker().clone());
                            Poll::Pending
                        } else {
                            return Poll::Ready(Err(std::io::Error::other("Slot not registered")));
                        }
                    } else if e.kind() == ErrorKind::Interrupted {
                        continue;
                    } else {
                        Poll::Ready(Err(e))
                    }
                }
            };
        }
    }
}

impl<'a> Future for AcceptFut<'a> {
    type Output = Result<(AsyncTcpStream, SocketAddr), std::io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        return match self.listener_handle.inner.accept() {
            Ok((stream, socket_addr)) => Poll::Ready(Ok((
                AsyncTcpStream::new(&self.listener_handle.handle, stream)?,
                socket_addr,
            ))),
            Err(e) => {
                if e.kind() == ErrorKind::WouldBlock {
                    println!("Parking...");
                    if let Some(io_slot) = self
                        .listener_handle
                        .handle
                        .io_slots
                        .lock()
                        .unwrap()
                        .get_mut(&self.listener_handle.token)
                    {
                        io_slot.read = Some(cx.waker().clone());
                        return Poll::Pending;
                    } else {
                        return Poll::Ready(Err(std::io::Error::other("Slot not registered")));
                    }
                } else {
                    return Poll::Ready(Err(e));
                }
            }
        };
    }
}

impl TimerHandle {
    fn sleep(&self, duration: Duration) -> Sleep {
        let deadline = Instant::now() + duration;
        let entry = Mutex::new((deadline, None));
        let handle = Arc::new(entry);
        let sleep_handle = handle.clone();
        self.timers.lock().unwrap().push(handle);
        Sleep::new(deadline, sleep_handle)
    }

    fn bind(&self, addr: SocketAddr) -> io::Result<AsyncTcpListener> {
        let mut tcp_listener = mio::net::TcpListener::bind(addr)?;
        let token = Token(self.token_counter.fetch_add(1, Relaxed));
        self.registry
            .register(&mut tcp_listener, token, Interest::READABLE)?;
        self.io_slots.lock().unwrap().insert(
            token,
            IoSlots {
                read: None,
                write: None,
            },
        );

        Ok(AsyncTcpListener {
            inner: tcp_listener,
            token,
            handle: self.clone(),
        })
    }
}

#[derive(Debug)]
struct Canceled;

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, Canceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut handle = self.handle.lock().unwrap();
        let result = std::mem::replace(&mut handle.result, ResultState::Running);
        match result {
            ResultState::Finished(result) => return Poll::Ready(Ok(result)),
            ResultState::Running => {
                handle.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            ResultState::Canceled => Poll::Ready(Err(Canceled)),
        }
    }
}

struct IoSlots {
    read: Option<Waker>,
    write: Option<Waker>,
}

struct Runtime {
    rx: Receiver<Arc<Task>>,
    tx: Sender<Arc<Task>>,
    next_id: AtomicU64,
    tasks: Mutex<HashMap<u64, Arc<Task>>>,
    timers: Arc<Mutex<Vec<Arc<Mutex<(Instant, Option<Waker>)>>>>>,
    poll: Mutex<MPoll>,
    waker: Arc<MWaker>,
    io_slots: Arc<Mutex<HashMap<Token, IoSlots>>>,
    registry: Arc<Registry>,
    token_counter: Arc<AtomicUsize>,
}

impl Drop for Runtime {
    fn drop(&mut self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());

        for (_id, value) in tasks {
            let fut = std::mem::take(&mut *value.future.lock().unwrap());
            drop(fut)
        }
    }
}

impl Runtime {
    fn new() -> Self {
        let (tx, rx) = channel::<Arc<Task>>();
        let poll = match MPoll::new() {
            Ok(poll) => poll,
            Err(e) => panic!("Failed to create the poll instance"),
        };
        let token_counter = Arc::new(AtomicUsize::new(1));
        let reserved_token = Token(0);
        let waker = match MWaker::new(poll.registry(), reserved_token) {
            Ok(waker) => waker,
            Err(_) => panic!("unable to create waker"),
        };

        let io_slots = Arc::new(Mutex::new(HashMap::new()));
        let registry = match poll.registry().try_clone() {
            Ok(v) => v,
            Err(_) => panic!("Unable to create an OS registry"),
        };

        Self {
            rx,
            tx,
            next_id: AtomicU64::new(0),
            tasks: Mutex::new(HashMap::new()),
            timers: Arc::new(Mutex::new(Vec::new())),
            poll: Mutex::new(poll),
            waker: Arc::new(waker),
            io_slots,
            registry: Arc::new(registry),
            token_counter,
        }
    }

    fn timer_handle(&self) -> TimerHandle {
        let timers = self.timers.clone();
        let io_slots = self.io_slots.clone();
        let registry = self.registry.clone();
        let token_counter = self.token_counter.clone();
        TimerHandle {
            timers,
            io_slots,
            registry,
            token_counter,
        }
    }

    fn get_next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn remove_task(&self, id: u64) {
        self.tasks.lock().unwrap().remove(&id);
    }

    fn block_on<T>(&self, fut: impl Future<Output = T>) -> T {
        let mut pinned_fut = pin!(fut);

        let thread_waker = Arc::new(ThreadWaker {
            // thread: t,
            waker: self.waker.clone(),
            is_main_future: AtomicBool::new(true),
        });
        let tw1 = thread_waker.clone();
        let waker = thread_waker.into();
        let mut context = Context::from_waker(&waker);

        let mut events = Events::with_capacity(1024);

        loop {
            let mut timer_list = self.timers.lock().unwrap();
            let mut elapsed_wakers = Vec::new();
            let now = Instant::now();
            timer_list.retain(|t| {
                let mut t_lock = t.lock().unwrap();
                if now >= t_lock.0 {
                    if let Some(w) = t_lock.1.take() {
                        elapsed_wakers.push(w);
                    }
                    false
                } else {
                    true
                }
            });
            drop(timer_list);

            for waker in elapsed_wakers {
                waker.wake()
            }

            if tw1.is_main_future.swap(false, Ordering::Relaxed) {
                match pinned_fut.as_mut().poll(&mut context) {
                    Poll::Ready(res) => return res,
                    Poll::Pending => {}
                }
            }

            let mut tasks: Vec<Arc<Task>> = Vec::new();

            loop {
                match self.rx.try_recv() {
                    Ok(task) => {
                        tasks.push(task);
                    }
                    Err(_) => break,
                }
            }

            for task in tasks {
                let waker = task.clone().into();
                let mut cx = Context::from_waker(&waker);

                let mut task_guard = task.future.lock().unwrap();
                // add condition to poll so we don't poll a complete future
                if let Some(task_pin) = task_guard.as_mut() {
                    if task_pin.as_mut().poll(&mut cx).is_ready() {
                        // Remove from the pool of tasks so it will not be polled again
                        self.remove_task(task.id);
                        // std::mem::replace(&mut task_guard.as_mut(), None);
                        *task_guard = None;
                    }
                };
            }

            let mut nearest_deadline = None;
            let timer_list = self.timers.lock().unwrap();

            let now = Instant::now();
            for timer in timer_list.iter() {
                let t_lock = timer.lock().unwrap();
                let time_to_deadline = t_lock.0.saturating_duration_since(now);
                if let Some(deadline) = nearest_deadline {
                    if time_to_deadline < deadline {
                        nearest_deadline = Some(time_to_deadline);
                    }
                } else {
                    nearest_deadline = Some(time_to_deadline);
                }

                drop(t_lock);
            }

            drop(timer_list);

            loop {
                match self
                    .poll
                    .lock()
                    .unwrap()
                    .poll(&mut events, nearest_deadline)
                {
                    Ok(_) => break,
                    Err(e) => {
                        if e.kind() == ErrorKind::Interrupted {
                            continue;
                        } else {
                            panic!("unable to wait for events")
                        }
                    }
                }
            }

            let mut io_slot_lock = self.io_slots.lock().unwrap();
            let mut wakers = Vec::new();
            for event in &events {
                if event.token() == Token(0) {
                    continue;
                } else {
                    if let Some(io_slot) = io_slot_lock.get_mut(&event.token()) {
                        if event.is_readable() {
                            if let Some(read_waker) = io_slot.read.take() {
                                wakers.push(read_waker);
                            }
                        }

                        if event.is_writable() {
                            if let Some(write_waker) = io_slot.write.take() {
                                wakers.push(write_waker);
                            }
                        }
                    }
                }
            }

            drop(io_slot_lock);

            for waker in wakers {
                waker.wake();
            }
        }
    }

    fn spawn<T>(&self, fut: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let handle = Arc::new(Mutex::new(HandleResult {
            result: ResultState::Running,
            waker: None,
        }));

        let cancel_guard = CancelGuard {
            slot: handle.clone(),
        };

        let wrapped_result = async move {
            let out = fut.await;
            let mut handle_guard = cancel_guard.slot.lock().unwrap();
            handle_guard.result = ResultState::Finished(out);

            // get the value of the waker atomically whie setting the result.
            let waker = handle_guard.waker.take();

            // dropped the guard first.
            drop(handle_guard);

            match waker {
                Some(waker) => {
                    waker.wake();
                }
                None => {}
            }
        };

        let boxed_fut = Box::pin(wrapped_result);
        let task_id = self.get_next_id();
        let task = Arc::new(Task {
            id: task_id,
            future: Mutex::new(Some(boxed_fut)),
            tx: self.tx.clone(),
            waker: self.waker.clone(),
        });

        self.tasks.lock().unwrap().insert(task_id, task.clone());

        self.tx.send(task).unwrap();

        JoinHandle { handle }
    }
}

// A task is created by a spawned top level feature
// a waker is contains a ptr to a task. used to signal
// that the task is ready to make progress

fn main() {
    let rt = Runtime::new();
    // let start = Instant::now();

    let ta = rt.timer_handle();
    // let mut conns = Vec::new();
    rt.block_on(async {
        let listener = ta.bind("127.0.0.1:8080".parse().unwrap()).unwrap();
        loop {
            let (mut stream, addr) = listener.accept().await.unwrap();
            println!("accepted {addr}");
            rt.spawn(async move {
                let mut buf= [0u8; 1024];
                'conn: loop {
                    let n = match stream.read(&mut buf).await {
                        Ok(n) => n,
                        Err(_e) => break
                    }; 

                    if n == 0 {break}
                    let mut written = 0usize;

                    while written < n {
                        let m = match stream.write(&buf[written..n]).await {
                            Ok(v) => v,
                            Err(_) => break 'conn
                        };

                        if m == 0 {break 'conn};
                        
                        written += m
                    }
                }

            });
        }
        // for i in 0..3 {
            //     ta.sleep(Duration::from_millis(100)).await;
            //     println!("[{:?}] A tick {i}", start.elapsed())
            // }
        });

        // for stream in &conns {
        //     rt.spawn(async move {
        //         let buf: &mut [u8] = &mut [];
        //         stream.read(buf);
        //     });
        // }

    // rt.block_on(a);

    // let tb = rt.timer_handle();
    // let b = rt.spawn(async move {
    //     for i in 0..3 {
    //         tb.sleep(Duration::from_millis(150)).await;
    //         println!("[{:?}] B tick {i}", start.elapsed())
    //     }
    // });

    // let tm = rt.timer_handle();
    // rt.block_on(async move {
    //     let _ = a.await;
    //     let _ = b.await;
    //     tm.sleep(Duration::from_millis(50)).await;
    //     println!("[{:?}] main done", start.elapsed());
    // });

    // let can_a = ChewingGumCan::new("Can A", 5).unwrap();
    // let can_b = ChewingGumCan::new("Can B", 5).unwrap();
    // let can_c = ChewingGumCan::new("Can C", 2).unwrap();

    // // Each spawn creates a separate task the workers can pick up

    // // let main_task = rt.block_on(can_c);
    // let task_a = rt.spawn(can_a);
    // let task_b = rt.spawn(can_b);
    // println!("[main] started, spawning A and B");

    // let c = rt.block_on(can_c);
    // println!("main done: {c:?}");
    // drop(rt);
    // println!("{:?}", Runtime::new().block_on(task_a));
}
