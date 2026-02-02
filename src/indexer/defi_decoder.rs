use alloy::primitives::U256;
use alloy::rpc::types::Log;
use alloy::sol;
use alloy::sol_types::SolEvent;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use std::str::FromStr;

// ============================================================
// DeFi Event Signatures
// ============================================================

sol! {
    // Uniswap V2 / SushiSwap / forks
    event UniV2Swap(
        address indexed sender,
        uint256 amount0In,
        uint256 amount1In,
        uint256 amount0Out,
        uint256 amount1Out,
        address indexed to
    );

    // Uniswap V3
    event UniV3Swap(
        address indexed sender,
        address indexed recipient,
        int256 amount0,
        int256 amount1,
        uint160 sqrtPriceX96,
        uint128 liquidity,
        int24 tick
    );

    // Curve TokenExchange
    event TokenExchange(
        address indexed buyer,
        int128 sold_id,
        uint256 tokens_sold,
        int128 bought_id,
        uint256 tokens_bought
    );

    // Aave V3 Supply
    event AaveSupply(
        address indexed reserve,
        address user,
        address indexed onBehalfOf,
        uint256 amount,
        uint16 indexed referralCode
    );

    // Aave V3 Borrow
    event AaveBorrow(
        address indexed reserve,
        address user,
        address indexed onBehalfOf,
        uint256 amount,
        uint8 interestRateMode,
        uint256 borrowRate,
        uint16 indexed referralCode
    );

    // Aave V3 Repay
    event AaveRepay(
        address indexed reserve,
        address indexed user,
        address indexed repayer,
        uint256 amount,
        bool useATokens
    );

    // Aave V3 LiquidationCall
    event LiquidationCall(
        address indexed collateralAsset,
        address indexed debtAsset,
        address indexed user,
        uint256 debtToCover,
        uint256 liquidatedCollateralAmount,
        address liquidator,
        bool receiveAToken
    );

    // Compound V3 (Comet) Supply
    event CometSupply(
        address indexed from,
        address indexed dst,
        uint256 amount
    );

    // Compound V3 (Comet) Withdraw
    event CometWithdraw(
        address indexed src,
        address indexed to,
        uint256 amount
    );

    // Compound V3 AbsorbCollateral
    event AbsorbCollateral(
        address indexed absorber,
        address indexed borrower,
        address indexed asset,
        uint256 collateralAbsorbed,
        uint256 usdValue
    );
}

// ============================================================
// DefiEvent struct
// ============================================================

#[derive(Debug, Clone)]
pub struct DefiEvent {
    pub chain_id: i64,
    pub block_number: i64,
    pub tx_hash: Vec<u8>,
    pub log_index: i32,
    pub protocol: String,
    pub event_type: String,
    pub contract_address: Vec<u8>,
    pub account: Option<Vec<u8>>,
    pub token_in: Option<Vec<u8>>,
    pub token_out: Option<Vec<u8>>,
    pub amount_in: Option<BigDecimal>,
    pub amount_out: Option<BigDecimal>,
    pub block_timestamp: DateTime<Utc>,
    pub raw_data: Option<serde_json::Value>,
}

// ============================================================
// Main decoder
// ============================================================

/// Decode all recognized DeFi events from a set of logs.
pub fn decode_defi_logs(
    logs: &[Log],
    block_timestamp: DateTime<Utc>,
    chain_id: i64,
) -> Vec<DefiEvent> {
    let mut events = Vec::new();
    for log in logs {
        if let Some(evt) = try_decode_log(log, block_timestamp, chain_id) {
            events.push(evt);
        }
    }
    events
}

fn try_decode_log(log: &Log, block_timestamp: DateTime<Utc>, chain_id: i64) -> Option<DefiEvent> {
    let topics = log.inner.data.topics();
    if topics.is_empty() {
        return None;
    }

    let sig = topics[0];
    let block_number = log.block_number.unwrap_or(0) as i64;
    let tx_hash = log.transaction_hash.unwrap_or_default().as_slice().to_vec();
    let log_index = log.log_index.unwrap_or(0) as i32;
    let contract_address = log.inner.address.as_slice().to_vec();

    if sig == UniV2Swap::SIGNATURE_HASH {
        decode_univ2_swap(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == UniV3Swap::SIGNATURE_HASH {
        decode_univ3_swap(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == TokenExchange::SIGNATURE_HASH {
        decode_curve_exchange(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == AaveSupply::SIGNATURE_HASH {
        decode_aave_supply(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == AaveBorrow::SIGNATURE_HASH {
        decode_aave_borrow(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == AaveRepay::SIGNATURE_HASH {
        decode_aave_repay(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == LiquidationCall::SIGNATURE_HASH {
        decode_aave_liquidation(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == CometSupply::SIGNATURE_HASH {
        decode_comet_supply(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == CometWithdraw::SIGNATURE_HASH {
        decode_comet_withdraw(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else if sig == AbsorbCollateral::SIGNATURE_HASH {
        decode_comet_absorb(log, chain_id, block_number, tx_hash, log_index, contract_address, block_timestamp)
    } else {
        None
    }
}

// ============================================================
// Protocol decoders
// ============================================================

fn u256_to_bd(val: U256) -> BigDecimal {
    BigDecimal::from_str(&val.to_string()).unwrap_or_default()
}

fn i256_to_bd(val: alloy::primitives::I256) -> BigDecimal {
    BigDecimal::from_str(&val.to_string()).unwrap_or_default()
}

fn decode_univ2_swap(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = UniV2Swap::decode_log(&log.inner).ok()?;
    let sender = decoded.sender;
    let to = decoded.to;
    let amount0_in = u256_to_bd(decoded.amount0In);
    let amount1_in = u256_to_bd(decoded.amount1In);
    let amount0_out = u256_to_bd(decoded.amount0Out);
    let amount1_out = u256_to_bd(decoded.amount1Out);

    // Determine net in/out: whichever amountN_in is non-zero is the "in" side
    let (amt_in, amt_out) = if decoded.amount0In > U256::ZERO {
        (amount0_in.clone(), amount1_out.clone())
    } else {
        (amount1_in.clone(), amount0_out.clone())
    };

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "uniswap_v2".to_string(),
        event_type: "swap".to_string(),
        contract_address,
        account: Some(sender.as_slice().to_vec()),
        token_in: None, // pair address — would need token0()/token1() calls to resolve
        token_out: None,
        amount_in: Some(amt_in),
        amount_out: Some(amt_out),
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "sender": format!("0x{}", hex::encode(sender.as_slice())),
            "to": format!("0x{}", hex::encode(to.as_slice())),
            "amount0In": amount0_in.to_string(),
            "amount1In": amount1_in.to_string(),
            "amount0Out": amount0_out.to_string(),
            "amount1Out": amount1_out.to_string(),
        })),
    })
}

fn decode_univ3_swap(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = UniV3Swap::decode_log(&log.inner).ok()?;
    let sender = decoded.sender;
    let recipient = decoded.recipient;
    let amount0 = decoded.amount0;
    let amount1 = decoded.amount1;

    // In Uniswap V3: positive = token received by pool (user paid), negative = token sent by pool (user received)
    let (amt_in, amt_out) = if amount0.is_positive() {
        (i256_to_bd(amount0), i256_to_bd(-amount1))
    } else {
        (i256_to_bd(amount1), i256_to_bd(-amount0))
    };

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "uniswap_v3".to_string(),
        event_type: "swap".to_string(),
        contract_address,
        account: Some(sender.as_slice().to_vec()),
        token_in: None,
        token_out: None,
        amount_in: Some(amt_in),
        amount_out: Some(amt_out),
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "sender": format!("0x{}", hex::encode(sender.as_slice())),
            "recipient": format!("0x{}", hex::encode(recipient.as_slice())),
            "amount0": decoded.amount0.to_string(),
            "amount1": decoded.amount1.to_string(),
            "sqrtPriceX96": decoded.sqrtPriceX96.to_string(),
            "liquidity": decoded.liquidity.to_string(),
            "tick": decoded.tick.to_string(),
        })),
    })
}

fn decode_curve_exchange(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = TokenExchange::decode_log(&log.inner).ok()?;
    let buyer = decoded.buyer;
    let tokens_sold = u256_to_bd(decoded.tokens_sold);
    let tokens_bought = u256_to_bd(decoded.tokens_bought);

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "curve".to_string(),
        event_type: "swap".to_string(),
        contract_address,
        account: Some(buyer.as_slice().to_vec()),
        token_in: None,
        token_out: None,
        amount_in: Some(tokens_sold.clone()),
        amount_out: Some(tokens_bought.clone()),
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "buyer": format!("0x{}", hex::encode(buyer.as_slice())),
            "sold_id": decoded.sold_id.to_string(),
            "tokens_sold": tokens_sold.to_string(),
            "bought_id": decoded.bought_id.to_string(),
            "tokens_bought": tokens_bought.to_string(),
        })),
    })
}

fn decode_aave_supply(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = AaveSupply::decode_log(&log.inner).ok()?;
    let reserve = decoded.reserve;
    let on_behalf_of = decoded.onBehalfOf;
    let amount = u256_to_bd(decoded.amount);

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "aave_v3".to_string(),
        event_type: "supply".to_string(),
        contract_address,
        account: Some(on_behalf_of.as_slice().to_vec()),
        token_in: Some(reserve.as_slice().to_vec()),
        token_out: None,
        amount_in: Some(amount),
        amount_out: None,
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "reserve": format!("0x{}", hex::encode(reserve.as_slice())),
            "user": format!("0x{}", hex::encode(decoded.user.as_slice())),
            "onBehalfOf": format!("0x{}", hex::encode(on_behalf_of.as_slice())),
        })),
    })
}

fn decode_aave_borrow(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = AaveBorrow::decode_log(&log.inner).ok()?;
    let reserve = decoded.reserve;
    let on_behalf_of = decoded.onBehalfOf;
    let amount = u256_to_bd(decoded.amount);

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "aave_v3".to_string(),
        event_type: "borrow".to_string(),
        contract_address,
        account: Some(on_behalf_of.as_slice().to_vec()),
        token_in: None,
        token_out: Some(reserve.as_slice().to_vec()),
        amount_in: None,
        amount_out: Some(amount),
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "reserve": format!("0x{}", hex::encode(reserve.as_slice())),
            "user": format!("0x{}", hex::encode(decoded.user.as_slice())),
            "onBehalfOf": format!("0x{}", hex::encode(on_behalf_of.as_slice())),
            "interestRateMode": decoded.interestRateMode,
            "borrowRate": decoded.borrowRate.to_string(),
        })),
    })
}

fn decode_aave_repay(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = AaveRepay::decode_log(&log.inner).ok()?;
    let reserve = decoded.reserve;
    let user = decoded.user;
    let amount = u256_to_bd(decoded.amount);

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "aave_v3".to_string(),
        event_type: "repay".to_string(),
        contract_address,
        account: Some(user.as_slice().to_vec()),
        token_in: Some(reserve.as_slice().to_vec()),
        token_out: None,
        amount_in: Some(amount),
        amount_out: None,
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "reserve": format!("0x{}", hex::encode(reserve.as_slice())),
            "user": format!("0x{}", hex::encode(user.as_slice())),
            "repayer": format!("0x{}", hex::encode(decoded.repayer.as_slice())),
            "useATokens": decoded.useATokens,
        })),
    })
}

fn decode_aave_liquidation(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = LiquidationCall::decode_log(&log.inner).ok()?;
    let collateral = decoded.collateralAsset;
    let debt = decoded.debtAsset;
    let user = decoded.user;
    let debt_to_cover = u256_to_bd(decoded.debtToCover);
    let liquidated_collateral = u256_to_bd(decoded.liquidatedCollateralAmount);

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "aave_v3".to_string(),
        event_type: "liquidation".to_string(),
        contract_address,
        account: Some(user.as_slice().to_vec()),
        token_in: Some(debt.as_slice().to_vec()),
        token_out: Some(collateral.as_slice().to_vec()),
        amount_in: Some(debt_to_cover),
        amount_out: Some(liquidated_collateral),
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "collateralAsset": format!("0x{}", hex::encode(collateral.as_slice())),
            "debtAsset": format!("0x{}", hex::encode(debt.as_slice())),
            "user": format!("0x{}", hex::encode(user.as_slice())),
            "liquidator": format!("0x{}", hex::encode(decoded.liquidator.as_slice())),
            "receiveAToken": decoded.receiveAToken,
        })),
    })
}

fn decode_comet_supply(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = CometSupply::decode_log(&log.inner).ok()?;
    let from = decoded.from;
    let dst = decoded.dst;
    let amount = u256_to_bd(decoded.amount);

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "compound_v3".to_string(),
        event_type: "supply".to_string(),
        contract_address: contract_address.clone(),
        account: Some(from.as_slice().to_vec()),
        token_in: Some(contract_address),
        token_out: None,
        amount_in: Some(amount),
        amount_out: None,
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "from": format!("0x{}", hex::encode(from.as_slice())),
            "dst": format!("0x{}", hex::encode(dst.as_slice())),
        })),
    })
}

fn decode_comet_withdraw(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = CometWithdraw::decode_log(&log.inner).ok()?;
    let src = decoded.src;
    let to = decoded.to;
    let amount = u256_to_bd(decoded.amount);

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "compound_v3".to_string(),
        event_type: "withdraw".to_string(),
        contract_address: contract_address.clone(),
        account: Some(src.as_slice().to_vec()),
        token_in: None,
        token_out: Some(contract_address),
        amount_in: None,
        amount_out: Some(amount),
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "src": format!("0x{}", hex::encode(src.as_slice())),
            "to": format!("0x{}", hex::encode(to.as_slice())),
        })),
    })
}

fn decode_comet_absorb(
    log: &Log,
    chain_id: i64,
    block_number: i64,
    tx_hash: Vec<u8>,
    log_index: i32,
    contract_address: Vec<u8>,
    block_timestamp: DateTime<Utc>,
) -> Option<DefiEvent> {
    let decoded = AbsorbCollateral::decode_log(&log.inner).ok()?;
    let absorber = decoded.absorber;
    let borrower = decoded.borrower;
    let asset = decoded.asset;
    let collateral_absorbed = u256_to_bd(decoded.collateralAbsorbed);
    let usd_value = u256_to_bd(decoded.usdValue);

    Some(DefiEvent {
        chain_id,
        block_number,
        tx_hash,
        log_index,
        protocol: "compound_v3".to_string(),
        event_type: "liquidation".to_string(),
        contract_address,
        account: Some(borrower.as_slice().to_vec()),
        token_in: None,
        token_out: Some(asset.as_slice().to_vec()),
        amount_in: None,
        amount_out: Some(collateral_absorbed),
        block_timestamp,
        raw_data: Some(serde_json::json!({
            "absorber": format!("0x{}", hex::encode(absorber.as_slice())),
            "borrower": format!("0x{}", hex::encode(borrower.as_slice())),
            "asset": format!("0x{}", hex::encode(asset.as_slice())),
            "usdValue": usd_value.to_string(),
        })),
    })
}
