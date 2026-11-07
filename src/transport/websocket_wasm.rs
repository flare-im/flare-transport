//! Browser WebSocket transport (`web-sys`) for WASM targets.

use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use js_sys::{ArrayBuffer, Uint8Array};
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{BinaryType, CloseEvent, ErrorEvent, MessageEvent, WebSocket};

use crate::common::platform::{MonotonicInstant, monotonic_now, timeout, yield_to_event_loop};

use crate::common::error::{FlareError, Result};
use crate::transport::connection::Connection;
use crate::transport::events::{
    ArcObserver, ConnectionEvent, deliver_to_observer,
    notify_observers as notify_connection_observers,
    notify_observers_and_clear as notify_connection_observers_and_clear,
};

#[derive(Clone)]
struct SendWebSocket(WebSocket);

#[cfg(target_arch = "wasm32")]
unsafe impl Send for SendWebSocket {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for SendWebSocket {}

impl SendWebSocket {
    fn new(url: &str) -> Result<Self> {
        WebSocket::new(url)
            .map(|ws| Self(ws))
            .map_err(|_| FlareError::connection_failed(format!("WebSocket::new failed: {url}")))
    }

    fn send(&self, data: &[u8]) -> Result<()> {
        let ready_state = self.0.ready_state();
        if ready_state != WebSocket::OPEN {
            return Err(FlareError::connection_failed(format!(
                "WebSocket send skipped: ready_state={ready_state}"
            )));
        }

        self.0
            .send_with_u8_array(data)
            .map_err(|_| FlareError::connection_failed("WebSocket send failed".to_string()))
    }

    fn close(&self) {
        let _ = self.0.close();
    }

    fn set_binary_type(&self, ty: BinaryType) {
        self.0.set_binary_type(ty);
    }

    fn set_onopen(&self, handler: Option<&js_sys::Function>) {
        self.0.set_onopen(handler);
    }

    fn set_onmessage(&self, handler: Option<&js_sys::Function>) {
        self.0.set_onmessage(handler);
    }

    fn set_onclose(&self, handler: Option<&js_sys::Function>) {
        self.0.set_onclose(handler);
    }

    fn set_onerror(&self, handler: Option<&js_sys::Function>) {
        self.0.set_onerror(handler);
    }
}

pub struct WebSocketTransport {
    ws: SendWebSocket,
    observers: Arc<StdMutex<Vec<ArcObserver>>>,
    last_active: Arc<StdMutex<MonotonicInstant>>,
}

impl WebSocketTransport {
    pub async fn connect(url: &str) -> Result<Self> {
        // 让出一次调度，便于浏览器在 Network 面板记录 WS 握手。
        yield_to_event_loop().await;

        web_sys::console::log_1(&format!("[flare-core] WebSocket::new({url})").into());
        let ws = SendWebSocket::new(url)?;
        web_sys::console::log_1(&"[flare-core] WebSocket::new ok, waiting for onopen".into());
        ws.set_binary_type(BinaryType::Arraybuffer);

        // Observers must exist before handlers are registered so inbound frames are never dropped.
        let observers = Arc::new(StdMutex::new(Vec::new()));
        let last_active = Arc::new(StdMutex::new(monotonic_now()));

        let (open_tx, open_rx) = tokio::sync::oneshot::channel::<Result<()>>();
        let opening = Arc::new(StdMutex::new(Some(open_tx)));
        {
            let opening = Arc::clone(&opening);
            let onopen = Closure::wrap(Box::new(move || {
                if let Some(tx) = opening.lock().ok().and_then(|mut slot| slot.take()) {
                    let _ = tx.send(Ok(()));
                }
            }) as Box<dyn FnMut()>);
            ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
            onopen.forget();
        }

        {
            let observers = Arc::clone(&observers);
            let last_active = Arc::clone(&last_active);
            let onmessage = Closure::wrap(Box::new(move |event: MessageEvent| {
                let event_data = event.data();
                let data = if let Ok(array_buffer) = event_data.clone().dyn_into::<ArrayBuffer>() {
                    let array = Uint8Array::new(&array_buffer);
                    let mut buf = vec![0u8; array.length() as usize];
                    array.copy_to(&mut buf);
                    buf
                } else if let Ok(array) = event_data.clone().dyn_into::<Uint8Array>() {
                    let mut buf = vec![0u8; array.length() as usize];
                    array.copy_to(&mut buf);
                    buf
                } else if let Some(text) = event_data.as_string() {
                    text.into_bytes()
                } else {
                    return;
                };
                if let Ok(mut active) = last_active.lock() {
                    *active = monotonic_now();
                }
                // Dispatch synchronously from the browser callback — do not queue through mpsc,
                // which may not wake the Tokio LocalSet promptly in WASM.
                Self::dispatch_observers(&observers, &ConnectionEvent::Message(data));
            }) as Box<dyn FnMut(MessageEvent)>);
            ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
            onmessage.forget();
        }

        {
            let observers = Arc::clone(&observers);
            let opening = Arc::clone(&opening);
            let onclose = Closure::wrap(Box::new(move |event: CloseEvent| {
                let reason = if event.reason().is_empty() {
                    "Connection closed".to_string()
                } else {
                    event.reason()
                };
                if let Some(tx) = opening.lock().ok().and_then(|mut slot| slot.take()) {
                    let _ = tx.send(Err(FlareError::connection_failed(reason)));
                    return;
                }
                Self::dispatch_observers_and_clear(
                    &observers,
                    &ConnectionEvent::Disconnected(reason),
                );
            }) as Box<dyn FnMut(CloseEvent)>);
            ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
            onclose.forget();
        }

        {
            let observers = Arc::clone(&observers);
            let onerror = Closure::wrap(Box::new(move |_event: ErrorEvent| {
                if let Some(tx) = opening.lock().ok().and_then(|mut slot| slot.take()) {
                    let _ = tx.send(Err(FlareError::connection_failed(
                        "WebSocket error before opening".to_string(),
                    )));
                    return;
                }
                Self::dispatch_observers_and_clear(
                    &observers,
                    &ConnectionEvent::Error(FlareError::connection_failed(
                        "WebSocket error".to_string(),
                    )),
                );
            }) as Box<dyn FnMut(ErrorEvent)>);
            ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
            onerror.forget();
        }

        let result = timeout(std::time::Duration::from_secs(30), open_rx)
            .await
            .map_err(|_| FlareError::connection_timeout("WebSocket open timeout".to_string()))
            .and_then(|result| {
                result.map_err(|_| {
                    FlareError::connection_failed("WebSocket open channel closed".to_string())
                })
            })
            .and_then(|result| result);
        ws.set_onopen(None);
        if let Err(error) = result {
            ws.set_onmessage(None);
            ws.set_onclose(None);
            ws.set_onerror(None);
            ws.close();
            return Err(error);
        }
        web_sys::console::log_1(&"[flare-core] WebSocket onopen received".into());

        Ok(Self {
            ws,
            observers,
            last_active,
        })
    }

    fn dispatch_observers(
        observers_arc: &Arc<StdMutex<Vec<ArcObserver>>>,
        event: &ConnectionEvent,
    ) {
        notify_connection_observers(observers_arc, event, "wasm websocket observers");
    }

    fn dispatch_observers_and_clear(
        observers_arc: &Arc<StdMutex<Vec<ArcObserver>>>,
        event: &ConnectionEvent,
    ) {
        notify_connection_observers_and_clear(observers_arc, event, "wasm websocket observers");
    }
}

#[async_trait]
impl Connection for WebSocketTransport {
    fn add_observer(&mut self, observer: ArcObserver) {
        // 与原生传输一样隔离新观察者处理 Connected 时的 panic（见 `deliver_to_observer`）。
        deliver_to_observer(
            &observer,
            &ConnectionEvent::Connected,
            "wasm websocket add_observer",
        );
        if let Ok(mut observers) = self.observers.lock() {
            observers.push(observer);
        }
    }

    fn remove_observer(&mut self, observer: ArcObserver) {
        if let Ok(mut observers) = self.observers.lock() {
            observers.retain(|o| !Arc::ptr_eq(o, &observer));
        }
    }

    async fn send(&mut self, data: &[u8]) -> Result<()> {
        if let Ok(mut active) = self.last_active.lock() {
            *active = monotonic_now();
        }
        self.ws.send(data)
    }

    async fn close(&mut self) -> Result<()> {
        self.close_with_reason("Closed by client").await
    }

    async fn close_with_reason(&mut self, reason: &str) -> Result<()> {
        self.ws.set_onopen(None);
        self.ws.set_onmessage(None);
        self.ws.set_onclose(None);
        self.ws.set_onerror(None);
        self.ws.close();
        Self::dispatch_observers_and_clear(
            &self.observers,
            &ConnectionEvent::Disconnected(reason.to_string()),
        );
        Ok(())
    }

    fn last_active_time(&self) -> MonotonicInstant {
        self.last_active
            .lock()
            .map(|guard| *guard)
            .unwrap_or_else(|_| monotonic_now())
    }

    fn update_active_time(&mut self) {
        if let Ok(mut active) = self.last_active.lock() {
            *active = monotonic_now();
        }
    }
}

#[cfg(target_arch = "wasm32")]
unsafe impl Send for WebSocketTransport {}
#[cfg(target_arch = "wasm32")]
unsafe impl Sync for WebSocketTransport {}
