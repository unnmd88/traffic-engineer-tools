use crate::snmp::{
    builders::{to_i32, to_stage_u405, to_stage_val_swarco_8stages},
    oid_metadata::{AccessType, OidKind, OidMetadata},
    parsers::{
        parse_ug405_stage, site_id_ug405_potok,
        stage_stcip::{parse_stcip_stage_potok, parse_stcip_stage_swarco},
    },
    registry::{
        SET_STAGE_ALIASES, STAGE_ALIASES,
        oids::{OPERATION_MODE_ALIASES, TO_BIT_ALIASES},
    },
    value::SnmpValueType,
};

/// utcReplySiteID for Potok — колонка с фикс. индексом 0, OID уже полный.
pub const UTC_REPLY_SITE_ID_POTOK_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.13267.3.2.5.1.1.2.0",
    name: "utcReplySiteID",
    aliases: &["scn"],
    description: "Identifies which, of possibly several, equipment at this site the\nobject should be applied to. The format is a Free Text ASCII String\nTypically this could be an SCN, IP address, or a number. Potok use only index 0",
    kind: OidKind::Exact,
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::OctetString,
    parser: Some(site_id_ug405_potok),
    builder: None,
};

/// utcReplyGn for UG405 — текущая фаза (read), индекс = SCN.
pub const UTC_REPLY_GN_UG405_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.13267.3.2.5.1.1.3",
    name: "utcReplyGn",
    aliases: STAGE_ALIASES,
    description: "Condition 1 confirms that a particular stage, or phase if specified is running.\nG1 and G2 shall normally be returned simultaneously to indicate that one of the following has occurred:\na) the mains supply to the signal aspects is off;\nb) manual method of traffic control is either in operation or requested;\nc) The traffic controller is switched off;\nd) The traffic controller has failed or shut down due to a fault;\ne) The interface between the OTU and the controller has been disconnected.",
    kind: OidKind::ScnIndexed,
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::OctetString,
    parser: Some(parse_ug405_stage),
    builder: None,
};

/// utcReplyGn for UTMC (пока не задействован ни в одном профиле).
pub const UTC_REPLY_GN_UTMC_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.13267.3.2.5.1.1.3",
    name: "utcReplyGn",
    aliases: STAGE_ALIASES,
    description: "Condition 1 confirms that a particular stage, or phase if specified is running.\nG1 and G2 shall normally be returned simultaneously to indicate that one of the following has occurred:\na) the mains supply to the signal aspects is off;\nb) manual method of traffic control is either in operation or requested;\nc) The traffic controller is switched off;\nd) The traffic controller has failed or shut down due to a fault;\ne) The interface between the OTU and the controller has been disconnected.",
    kind: OidKind::Exact,
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::OctetString,
    parser: Some(parse_ug405_stage),
    builder: None,
};

/// swarcoUTCTrafftechPhaseStatus for Swarco — текущая фаза (read).
pub const SWARCO_UTC_TRAFFTECH_PHASE_STATUS_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.1618.3.7.2.11.2",
    name: "swarcoUTCTrafftechPhaseStatus",
    aliases: STAGE_ALIASES,
    description: "Current phase",
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::Unsigned32,
    kind: OidKind::Scalar,
    parser: Some(parse_stcip_stage_swarco),
    builder: None,
};

/// swarcoUTCTrafftechPhaseCommand for Swarco — команда фазы (write).
pub const SWARCO_UTC_TRAFFTECH_PHASE_COMMAND_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.1618.3.7.2.11.1",
    name: "swarcoUTCTrafftechPhaseCommand",
    aliases: SET_STAGE_ALIASES,
    description: "Commands the controller to go to the specified phase.\nA phase of 0 means no phase commanded.",
    access: AccessType::ReadWrite,
    syntax: SnmpValueType::Unsigned32,
    kind: OidKind::Scalar,
    parser: None,
    builder: Some(to_stage_val_swarco_8stages),
};

/// swarcoUTCTrafftechPhaseStatus for Potok — текущая фаза (read).
pub const POTOKS_UTC_TRAFFTECH_PHASE_STATUS_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.1618.3.7.2.11.2",
    name: "swarcoUTCTrafftechPhaseStatus",
    aliases: STAGE_ALIASES,
    description: "Current phase",
    access: AccessType::ReadOnly,
    syntax: SnmpValueType::Unsigned32,
    kind: OidKind::Scalar,
    parser: Some(parse_stcip_stage_potok),
    builder: None,
};

/// utcType2OperationMode — скаляр.
pub const UTC_TYPE2_OPERATION_MODE_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.13267.3.2.4.1",
    name: "utcType2OperationMode",
    aliases: OPERATION_MODE_ALIASES,
    description: "Tells the Outstation what mode to operate in;\ntells the Instation the current mode of the Outstation.\nThe Outstation will only accept changes to the next greater or any lesser value\ni.e. standalone to monitor or monitor to utccontrol the outstation must\nreject (as bad value?) requests that increment the value by more than 1.\nIn standalone mode all output bits are set to zero. Reply bits are not sent but can be polled.\nIn monitor mode all output bits are set to zero. The OTU sends inform requests to the Instation as defined elsewhere within this MIB.\nIn utccontrol mode inform requests are sent as in monitor mode and output bits are controlled by an external system.",
    access: AccessType::ReadWrite,
    syntax: SnmpValueType::Integer,
    kind: OidKind::Scalar,
    parser: None,
    builder: Some(to_i32),
};

/// utcControlTO — колонка, индекс = SCN.
pub const UTC_CONTROL_TO_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.13267.3.2.4.2.1.15",
    name: "utcControlTO",
    aliases: TO_BIT_ALIASES,
    description: "This facility shall allow control to be accepted from a remote source.\nWhile the TO bit is set to logic 0 (inactive condition) the controller\nshall ignore the control bits specified in an associated works specification.\nWhere an ancillary MOVA unit is specified and control is via the UTC\ninterface, control shall only be operational when the Take Over bit\n(logic condition 1) is present",
    access: AccessType::ReadWrite,
    syntax: SnmpValueType::Integer,
    kind: OidKind::ScnIndexed,
    parser: None,
    builder: Some(to_i32),
};

/// utcControlFn — команда/форсирование фазы (write), индекс = SCN.
pub const UTC_CONTROL_FN_METADATA: OidMetadata = OidMetadata {
    oid: "1.3.6.1.4.1.13267.3.2.4.2.1.5",
    name: "utcControlFn",
    aliases: SET_STAGE_ALIASES,
    description: "Condition 1 shall force the controller to make an immediate change to the selected stage or shall hold a selected stage subject to the following conditions:\na) if the selected stage does not have rightofway then condition 1 on the force bit for that stage, and no other, shall cause a forced change to that stage provided that a demand exists or is assumed to exist for the stage;\nb) if the controller is in an intergreen or a minimum green period, the change to the selected stage shall be deferred until the expiry of the minimum green period, provided that the force condition still exists;\nc) if the selected stage has already appeared, condition 1 on the force bit for that stage shall reset the phase maximum timers and hold that stage for so long as the condition 1 is received, provided that gap changes to another demanded stage are prevented by vehicle extensions (e.g. either by control demand signals or from local detectors).",
    access: AccessType::ReadWrite,
    syntax: SnmpValueType::OctetString,
    kind: OidKind::ScnIndexed,
    parser: Some(parse_ug405_stage),
    builder: Some(to_stage_u405),
};
