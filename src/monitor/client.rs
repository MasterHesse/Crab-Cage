use super::*;
use std::sync::{Mutex, Arc};
use std::collections::HashMap;

#[derive(Clone, Default)]
pub struct ClientTracker {
    clients: Arc<Mutex<HashMap<u64, ClientInfo>>>,
    next_id: Arc<AtomicU64>,
}

impl ClientTracker {
    pub fn new() -> Self{
        ClientTracker { 
            clients: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)), 
        }
    }

    pub fn add_client(&self, addr: SocketAddr) -> u64 {
        let mut clients = self.clients.lock().unwrap();
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        clients.insert(
            id, 
            ClientInfo { 
                addr,
                connect_time: Instant::now(), 
                last_command: "None".to_string(), 
                last_command_time: Instant::now(), 
            }
        );

        id
    }

    pub fn remove_client(&self, id:u64) {
        let mut clients = self.clients.lock().unwrap();
        clients.remove(&id);
    }

    pub fn update_command(&self, id: u64, command: &str) {
        let mut clients = self.clients.lock().unwrap();
        if let Some(client) = clients.get_mut(&id) {
            client.last_command = command.to_string();
            client.last_command_time = Instant::now();
        }
    }

    pub fn list_clients(&self) -> String {
        let clients = self.clients.lock().unwrap();
        let mut response = String::new();

        for (id, client) in clients.iter() {
            response.push_str(&format!(
                "id={} addr={} age={}s idle={}s cmd={}\n",
                id,
                client.addr,
                client.connect_time.elapsed().as_secs(),
                client.last_command_time.elapsed().as_secs(),
                client.last_command
            ));
        }

        response
    }
}