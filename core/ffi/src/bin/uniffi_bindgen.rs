//! `uniffi-bindgen` entry point: generates Kotlin/Swift sources from the
//! compiled `stream_ffi` library. See `../README.md` for the exact commands.

fn main() {
    uniffi::uniffi_bindgen_main();
}
