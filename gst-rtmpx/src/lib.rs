mod common;
mod rtmpxsink;
mod rtmpxsrc;

use gst::glib;

fn plugin_init(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
  rtmpxsrc::register(plugin)?;
  rtmpxsink::register(plugin)?;
  Ok(())
}

gst::plugin_define!(
  rtmpx,
  env!("CARGO_PKG_DESCRIPTION"),
  plugin_init,
  concat!(env!("CARGO_PKG_VERSION"), "-", env!("COMMIT_ID")),
  "MIT/X11",
  env!("CARGO_PKG_NAME"),
  env!("CARGO_PKG_NAME"),
  "https://github.com/darfink/gst-rtmpx",
  env!("BUILD_REL_DATE")
);
