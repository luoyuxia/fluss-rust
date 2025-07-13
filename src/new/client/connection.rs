use crate::admin::admin::FlussAdmin;
use crate::args::Args;
use crate::new::client::metadata::Metadata;
use crate::new::client::table::FlussTable;
use crate::new::connection::connection::Connections;
use std::rc::Rc;
use std::sync::Arc;

struct FlussConnection {
    metadata: Arc<Metadata>,
    network_connects: Arc<Connections>,
    args: Rc<Args>,
}

impl FlussConnection {
    fn new(args: &Args) -> Self {
        FlussConnection {
            metadata: Arc::new(Metadata::default()),
            network_connects: Arc::new(Connections::default()),
            args: Rc::new(args.clone()),
        }
    }

    fn get_admin(&self) -> FlussAdmin {
        todo!()
    }

    fn get_table(&self) -> FlussTable {
        todo!()
    }
}
