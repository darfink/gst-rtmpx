mod imp;

use gst::glib;
use gst::prelude::*;

glib::wrapper! {
  pub struct RtmpxSrc(ObjectSubclass<imp::RtmpxSrc>)
    @extends gst_base::PushSrc, gst_base::BaseSrc, gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
  gst::Element::register(
    Some(plugin),
    "rtmpxsrc",
    gst::Rank::NONE,
    RtmpxSrc::static_type(),
  )
}
