//! IOx Query Server Implementation.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]
#![allow(dead_code)]

pub mod cache;
mod cache_system;
pub mod chunk;
mod database;
mod handler;
mod ingester;
mod namespace;
mod poison;
mod query_log;
mod server;
mod system_tables;
mod table;
mod tombstone;

pub use database::QuerierDatabase;
pub use handler::{QuerierHandler, QuerierHandlerImpl};
pub use ingester::{
    create_ingester_connection, create_ingester_connection_for_testing, IngesterConnection,
};
pub use namespace::QuerierNamespace;
pub use server::QuerierServer;
