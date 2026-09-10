mod imp;

use gst::glib;
use gst::prelude::*;

glib::wrapper! {
  pub struct RtmpxSink(ObjectSubclass<imp::RtmpxSink>)
    @extends gst_base::BaseSink, gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
  gst::Element::register(
    Some(plugin),
    "rtmpxsink",
    gst::Rank::NONE,
    RtmpxSink::static_type(),
  )
}
