// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

//! The connection keeps its pacing timer in step with the congestion controller: a departure
//! time that an ACK moves earlier releases transmission at the new time, not the old one.

use super::*;
use s2n_quic::provider::congestion_controller::Endpoint;
use s2n_quic_core::{
    random,
    recovery::{
        congestion_controller::{CongestionController, PathInfo, Publisher},
        RttEstimator,
    },
    time::Timestamp,
};

/// Packets the client sends before the controller starts holding transmission back
const PACKETS_BEFORE_HOLD: usize = 20;
/// How far ahead the controller first sets the departure time
const HOLD: Duration = Duration::from_secs(2);
/// The departure time after the next ACK, relative to that ACK
const RELEASE_AFTER_ACK: Duration = Duration::from_millis(40);

#[derive(Debug, Default)]
struct State {
    packets_sent: usize,
    hold_until: Option<Timestamp>,
    released_at: Option<Timestamp>,
    /// When false, an ACK does not move the departure time (the control case)
    move_on_ack: bool,
    first_send_after_hold: Option<Timestamp>,
}

#[derive(Clone, Debug)]
struct Holding(Arc<Mutex<State>>);

impl Endpoint for Holding {
    type CongestionController = Holding;

    fn new_congestion_controller(&mut self, _path_info: PathInfo) -> Self::CongestionController {
        self.clone()
    }
}

impl CongestionController for Holding {
    type PacketInfo = ();

    fn congestion_window(&self) -> u32 {
        u32::MAX
    }

    fn bytes_in_flight(&self) -> u32 {
        0
    }

    fn is_congestion_limited(&self) -> bool {
        false
    }

    fn requires_fast_retransmission(&self) -> bool {
        false
    }

    fn on_packet_sent<Pub: Publisher>(
        &mut self,
        time_sent: Timestamp,
        _sent_bytes: usize,
        _app_limited: Option<bool>,
        _rtt_estimator: &RttEstimator,
        _publisher: &mut Pub,
    ) {
        let mut state = self.0.lock().unwrap();
        state.packets_sent += 1;
        if state.hold_until.is_some() {
            state.first_send_after_hold.get_or_insert(time_sent);
        } else if state.packets_sent == PACKETS_BEFORE_HOLD {
            state.hold_until = Some(time_sent + HOLD);
        }
    }

    fn on_rtt_update<Pub: Publisher>(
        &mut self,
        _time_sent: Timestamp,
        _now: Timestamp,
        _rtt_estimator: &RttEstimator,
        _publisher: &mut Pub,
    ) {
    }

    fn on_ack<Pub: Publisher>(
        &mut self,
        _newest_acked_time_sent: Timestamp,
        _bytes_acknowledged: usize,
        _newest_acked_packet_info: Self::PacketInfo,
        _rtt_estimator: &RttEstimator,
        _random_generator: &mut dyn random::Generator,
        ack_receive_time: Timestamp,
        _publisher: &mut Pub,
    ) {
        let mut state = self.0.lock().unwrap();
        if state.move_on_ack && state.hold_until.is_some() && state.released_at.is_none() {
            state.released_at = Some(ack_receive_time + RELEASE_AFTER_ACK);
        }
    }

    fn on_packet_lost<Pub: Publisher>(
        &mut self,
        _lost_bytes: u32,
        _packet_info: Self::PacketInfo,
        _persistent_congestion: bool,
        _new_loss_burst: bool,
        _random_generator: &mut dyn random::Generator,
        _timestamp: Timestamp,
        _publisher: &mut Pub,
    ) {
    }

    fn on_explicit_congestion<Pub: Publisher>(
        &mut self,
        _ce_count: u64,
        _event_time: Timestamp,
        _publisher: &mut Pub,
    ) {
    }

    fn on_mtu_update<Pub: Publisher>(&mut self, _max_data_size: u16, _publisher: &mut Pub) {}

    fn on_packet_discarded<Pub: Publisher>(&mut self, _bytes_sent: usize, _publisher: &mut Pub) {}

    fn earliest_departure_time(&self) -> Option<Timestamp> {
        let state = self.0.lock().unwrap();
        // The first send after the hold ends it, so later sends are not held back
        if state.first_send_after_hold.is_some() {
            return None;
        }
        state.released_at.or(state.hold_until)
    }
}

fn run(move_on_ack: bool) -> State {
    let model = Model::default();
    let state = Arc::new(Mutex::new(State {
        move_on_ack,
        ..Default::default()
    }));
    let controller = Holding(state.clone());

    test(model.clone(), |handle| {
        let server = build_server(handle, model.clone())?;
        let addr = start_server(server)?;
        let client = Client::builder()
            .with_io(handle.builder().build().unwrap())?
            .with_tls(certificates::CERT_PEM)?
            .with_event(tracing_events(true, model.clone()))?
            .with_random(Random::with_seed(123))?
            .with_congestion_controller(controller)?
            .start()?;
        start_client(client, addr, Data::new(1_000_000))?;
        Ok(addr)
    })
    .unwrap();

    Arc::try_unwrap(state)
        .map(|state| state.into_inner().unwrap())
        .unwrap_or_else(|state| std::mem::take(&mut *state.lock().unwrap()))
}

#[test]
fn a_departure_time_moved_earlier_by_an_ack_releases_transmission() {
    let state = run(true);
    let hold_until = state.hold_until.expect("the hold started");
    let released_at = state.released_at.expect("an ACK moved the departure time");
    let first = state.first_send_after_hold.expect("sending resumed");
    assert!(
        first >= released_at,
        "sent before the controller allowed it"
    );
    assert!(
        first < hold_until - Duration::from_secs(1),
        "sending waited for the old departure time: resumed {first:?}, released {released_at:?}, old {hold_until:?}"
    );
}

#[test]
fn an_unchanged_departure_time_still_holds_transmission() {
    let state = run(false);
    let hold_until = state.hold_until.expect("the hold started");
    let first = state.first_send_after_hold.expect("sending resumed");
    assert!(first >= hold_until, "sent before the controller allowed it");
}
