use crate::snmp::{
    oid_metadata::{AccessType, OidMetadata, Requirenment},
    parsers::{parse_ug405_stage, site_id_ug405_potok, stage_stcip::parse_stcip_stage_swarco},
    registry::oids::STAGE_ALIAS,
    value::SnmpValueType,
};

/// swarcoUTCTrafftechPhaseStatus
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_OID: &str = "1.3.6.1.4.1.1618.3.7.2.11.2";
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_NAME: &str = "swarcoUTCTrafftechPhaseStatus";
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_ALIAS: &str = STAGE_ALIAS;
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_DESCRITION: &str = "Current phase";
/*-----------------------------------------------------------------*/

/// utcReplySiteID for Potok with index 0
pub const UTC_REPLY_SITE_ID_POTOK_OID: &str = "1.3.6.1.4.1.13267.3.2.5.1.1.2.0";
pub const UTC_REPLY_SITE_ID_POTOK_NAME: &str = "utcReplySiteID";
pub const UTC_REPLY_SITE_ID_POTOK_ALIAS: &str = "scn";
pub const UTC_REPLY_SITE_ID_POTOK_DESCRIPTION: &str =
    " Identifies which, of possibly several, equipment at this site the
object should be applied to. The format is a Free Text ASCII String
Typically this could be an SCN, IP address, or a number. Potok use only index 0";
/*-----------------------------------------------------------------*/

/// utcReplyGn
pub const UTC_REPLY_GN_OID: &str = "1.3.6.1.4.1.13267.3.2.5.1.1.3";
pub const UTC_REPLY_GN_NAME: &str = "utcReplyGn";
//pub const UTC_REPLY_GN_ALIAS: &str = STAGE_ALIAS;
pub const UTC_REPLY_GN_DESCRIPTION: &str =
    "Condition 1 confirms that a particular stage, or phase if specified is running.
G1 and G2 shall normally be returned simultaneously to indicate
                        
                        that one of the following has
                        occurred:
                        a) the mains supply to the signal
                        aspects is off;
                        b) manual method of traffic
                        control is either in operation or
                        requested;
                        c) The traffic controller is
                        switched off;
                        d) The traffic controller has failed
                        or shut down due to a fault;
                        e) The interface between the
                        OTU and the controller has
                        been disconnected.";
/*-----------------------------------------------------------------*/

///  swarcoUTCTrafftechPhaseCommand
pub const SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_OID: &str = "1.3.6.1.4.1.1618.3.7.2.11.1";
pub const SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_NAME: &str = "swarcoUTCTrafftechPhaseCommand";
pub const SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_ALIAS: &str = STAGE_ALIAS;
pub const SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_DESCRITION: &str =
    "Commands the controller to go to the specified phase.
A phase of 0 means no phase commanded.";
/*-----------------------------------------------------------------*/

///  utcControlFn
pub const UTC_CONTROL_FN_OID: &str = "1.3.6.1.4.1.13267.3.2.4.2.1.5";
pub const UTC_CONTROL_FN_OID_NAME: &str = "utcControlFn";
pub const UTC_CONTROL_FN_OID_ALIAS: &str = STAGE_ALIAS;
pub const UTC_CONTROL_FN_OID_DESCRIPTION: &str = "Condition 1 shall force the
controller to make an immediate
change to the selected stage or
shall hold a selected stage subject
to the following conditions:
a) if the selected stage does not
have rightofway then
condition 1 on the force bit for
that stage, and no other, shall
cause a forced change to that
stage provided that a demand
exists or is assumed to exist for
the stage;
b) if the controller is in an
intergreen or a minimum green
period, the change to the
selected stage shall be
deferred until the expiry of the
minimum green period,
provided that the force
condition still exists;
c) if the selected stage has
already appeared, condition 1
on the force bit for that stage
shall reset the phase maximum
timers and hold that stage for
so long as the condition 1 is
received, provided that gap
changes to another demanded
stage are prevented by vehicle
extensions (e.g. either by
control demand signals or from
local detectors).";
/*-----------------------------------------------------------------*/
