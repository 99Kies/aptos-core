// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use crate::{
    database::{
        clean_data_for_db, execute_with_better_error, get_chunks, PgDbPool, PgPoolConnection,
    },
    indexer::{
        errors::TransactionProcessingError, processing_result::ProcessingResult,
        transaction_processor::TransactionProcessor,
    },
    models::{
        block_metadata_transactions::BlockMetadataTransactionModel,
        events::EventModel,
        move_modules::MoveModule,
        move_resources::MoveResource,
        move_tables::{TableItem, TableMetadata},
        signatures::Signature,
        transactions::{TransactionDetail, TransactionModel},
        user_transactions::UserTransactionModel,
        write_set_changes::{WriteSetChangeDetail, WriteSetChangeModel},
    },
    models::coin_models::{
        coin_activities::{CoinActivity, CurrentCoinBalancePK},
        coin_balances::{CoinBalance, CurrentCoinBalance},
        coin_infos::{CoinInfo, CoinInfoQuery},
        coin_supply::CoinSupply,
    },
    models::token_models::{
        ans_lookup::{CurrentAnsLookup, CurrentAnsLookupPK},
        collection_datas::{CollectionData, CurrentCollectionData},
        token_activities::TokenActivity,
        token_claims::CurrentTokenPendingClaim,
        token_datas::{CurrentTokenData, TokenData},
        token_ownerships::{CurrentTokenOwnership, TokenOwnership},
        tokens::{CurrentTokenOwnershipPK, CurrentTokenPendingClaimPK, Token, TokenDataIdHash},
    },
    schema,
};
use aptos_api_types::Transaction;
use async_trait::async_trait;
use aptos_types::APTOS_COIN_TYPE;
use diesel::{pg::upsert::excluded, result::Error, ExpressionMethods, PgConnection};
use field_count::FieldCount;
use std::{collections::HashMap, fmt::Debug};

pub const NAME: &str = "all_processor";
pub struct AllTransactionProcessor {
    connection_pool: PgDbPool,
    ans_contract_address: Option<String>,
}

impl AllTransactionProcessor {
    pub fn new(connection_pool: PgDbPool, ans_contract_address: Option<String>) -> Self {
        aptos_logger::info!(
            ans_contract_address = ans_contract_address,
            "init TokenTransactionProcessor"
        );
        Self {
            connection_pool,
            ans_contract_address,
        }
    }
}

impl Debug for AllTransactionProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = &self.connection_pool.state();
        write!(
            f,
            "AllTransactionProcessor {{ connections: {:?}  idle_connections: {:?} }}",
            state.connections, state.idle_connections
        )
    }
}

fn insert_to_db(
    conn: &mut PgPoolConnection,
    name: &'static str,
    start_version: u64,
    end_version: u64,
    txns: Vec<TransactionModel>,
    txn_details: Vec<TransactionDetail>,
    events: Vec<EventModel>,
    wscs: Vec<WriteSetChangeModel>,
    wsc_details: Vec<WriteSetChangeDetail>,
    coin_activities: Vec<CoinActivity>,
    coin_infos: Vec<CoinInfo>,
    coin_balances: Vec<CoinBalance>,
    current_coin_balances: Vec<CurrentCoinBalance>,
    coin_supply: Vec<CoinSupply>,
    basic_token_transaction_lists: ( // token
                                     Vec<Token>,
                                     Vec<TokenOwnership>,
                                     Vec<TokenData>,
                                     Vec<CollectionData>,
    ),
    basic_token_current_lists: (
        Vec<CurrentTokenOwnership>,
        Vec<CurrentTokenData>,
        Vec<CurrentCollectionData>,
    ),
    token_activities: Vec<TokenActivity>,
    current_token_claims: Vec<CurrentTokenPendingClaim>,
    current_ans_lookups: Vec<CurrentAnsLookup>,
) -> Result<(), diesel::result::Error> {
    aptos_logger::trace!(
        name = name,
        start_version = start_version,
        end_version = end_version,
        "Inserting to db",
    );
    let (tokens, token_ownerships, token_datas, collection_datas) = basic_token_transaction_lists;
    let (current_token_ownerships, current_token_datas, current_collection_datas) =
        basic_token_current_lists;
    match conn
        .build_transaction()
        .read_write()
        .run::<_, Error, _>(|pg_conn| {
            insert_transactions(pg_conn, &txns)?;
            insert_user_transactions_w_sigs(pg_conn, &txn_details)?;
            insert_block_metadata_transactions(pg_conn, &txn_details)?;
            insert_events(pg_conn, &events)?;
            insert_write_set_changes(pg_conn, &wscs)?;
            insert_move_modules(pg_conn, &wsc_details)?;
            insert_move_resources(pg_conn, &wsc_details)?;
            insert_table_data(pg_conn, &wsc_details)?;
            insert_to_db_impl(
                pg_conn,
                &coin_activities,
                &coin_infos,
                &coin_balances,
                &current_coin_balances,
                &coin_supply,
                (&tokens, &token_ownerships, &token_datas, &collection_datas),
                (
                    &current_token_ownerships,
                    &current_token_datas,
                    &current_collection_datas,
                ),
                &token_activities,
                &current_token_claims,
                &current_ans_lookups,
            )
        }) {
        Ok(_) => Ok(()),
        Err(_) => conn
            .build_transaction()
            .read_write()
            .run::<_, Error, _>(|pg_conn| {
                let txns = clean_data_for_db(txns, true);
                let txn_details = clean_data_for_db(txn_details, true);
                let events = clean_data_for_db(events, true);
                let wscs = clean_data_for_db(wscs, true);
                let wsc_details = clean_data_for_db(wsc_details, true);

                insert_transactions(pg_conn, &txns)?;
                insert_user_transactions_w_sigs(pg_conn, &txn_details)?;
                insert_block_metadata_transactions(pg_conn, &txn_details)?;
                insert_events(pg_conn, &events)?;
                insert_write_set_changes(pg_conn, &wscs)?;
                insert_move_modules(pg_conn, &wsc_details)?;
                insert_move_resources(pg_conn, &wsc_details)?;
                insert_table_data(pg_conn, &wsc_details)?;

                let coin_activities = clean_data_for_db(coin_activities, true); //coin
                let coin_infos = clean_data_for_db(coin_infos, true);
                let coin_balances = clean_data_for_db(coin_balances, true);
                let current_coin_balances = clean_data_for_db(current_coin_balances, true);

                let tokens = clean_data_for_db(tokens, true); // token
                let token_datas = clean_data_for_db(token_datas, true);
                let token_ownerships = clean_data_for_db(token_ownerships, true);
                let collection_datas = clean_data_for_db(collection_datas, true);
                let current_token_ownerships = clean_data_for_db(current_token_ownerships, true);
                let current_token_datas = clean_data_for_db(current_token_datas, true);
                let current_collection_datas = clean_data_for_db(current_collection_datas, true);
                let token_activities = clean_data_for_db(token_activities, true);
                let current_token_claims = clean_data_for_db(current_token_claims, true);
                let current_ans_lookups = clean_data_for_db(current_ans_lookups, true);

                insert_to_db_impl(
                    pg_conn,
                    &coin_activities,
                    &coin_infos,
                    &coin_balances,
                    &current_coin_balances,
                    &coin_supply,
                    (&tokens, &token_ownerships, &token_datas, &collection_datas),
                    (
                        &current_token_ownerships,
                        &current_token_datas,
                        &current_collection_datas,
                    ),
                    &token_activities,
                    &current_token_claims,
                    &current_ans_lookups,
                )
            }),
    }
}

fn insert_transactions(
    conn: &mut PgConnection,
    txns: &[TransactionModel],
) -> Result<(), diesel::result::Error> {
    use schema::transactions::dsl::*;
    let chunks = get_chunks(txns.len(), TransactionModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::transactions::table)
                .values(&txns[start_ind..end_ind])
                .on_conflict(version)
                .do_update()
                .set((epoch.eq(excluded(epoch)),)),
            None,
        )?;
    }
    Ok(())
}

fn insert_user_transactions_w_sigs(
    conn: &mut PgConnection,
    txn_details: &[TransactionDetail],
) -> Result<(), diesel::result::Error> {
    use schema::{signatures::dsl as sig_schema, user_transactions::dsl as ut_schema};
    let mut all_signatures = vec![];
    let mut all_user_transactions = vec![];
    for detail in txn_details {
        if let TransactionDetail::User(user_txn, sigs) = detail {
            all_signatures.append(&mut sigs.clone());
            all_user_transactions.push(user_txn.clone());
        }
    }
    let chunks = get_chunks(
        all_user_transactions.len(),
        UserTransactionModel::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::user_transactions::table)
                .values(&all_user_transactions[start_ind..end_ind])
                .on_conflict(ut_schema::version)
                .do_update()
                .set((ut_schema::epoch.eq(excluded(ut_schema::epoch)),)),
            None,
        )?;
    }
    let chunks = get_chunks(all_signatures.len(), Signature::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::signatures::table)
                .values(&all_signatures[start_ind..end_ind])
                .on_conflict((
                    sig_schema::transaction_version,
                    sig_schema::multi_agent_index,
                    sig_schema::multi_sig_index,
                    sig_schema::is_sender_primary,
                ))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_block_metadata_transactions(
    conn: &mut PgConnection,
    txn_details: &[TransactionDetail],
) -> Result<(), diesel::result::Error> {
    use schema::block_metadata_transactions::dsl::*;

    let bmt = txn_details
        .iter()
        .filter_map(|detail| match detail {
            TransactionDetail::BlockMetadata(bmt) => Some(bmt.clone()),
            _ => None,
        })
        .collect::<Vec<BlockMetadataTransactionModel>>();

    let chunks = get_chunks(bmt.len(), BlockMetadataTransactionModel::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::block_metadata_transactions::table)
                .values(&bmt[start_ind..end_ind])
                .on_conflict(version)
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_events(conn: &mut PgConnection, ev: &[EventModel]) -> Result<(), diesel::result::Error> {
    use schema::events::dsl::*;

    let chunks = get_chunks(ev.len(), EventModel::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::events::table)
                .values(&ev[start_ind..end_ind])
                .on_conflict((account_address, creation_number, sequence_number))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_write_set_changes(
    conn: &mut PgConnection,
    wscs: &[WriteSetChangeModel],
) -> Result<(), diesel::result::Error> {
    use schema::write_set_changes::dsl::*;

    let chunks = get_chunks(wscs.len(), WriteSetChangeModel::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::write_set_changes::table)
                .values(&wscs[start_ind..end_ind])
                .on_conflict((transaction_version, index))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_move_modules(
    conn: &mut PgConnection,
    wsc_details: &[WriteSetChangeDetail],
) -> Result<(), diesel::result::Error> {
    use schema::move_modules::dsl::*;

    let modules = wsc_details
        .iter()
        .filter_map(|detail| match detail {
            WriteSetChangeDetail::Module(module) => Some(module.clone()),
            _ => None,
        })
        .collect::<Vec<MoveModule>>();

    let chunks = get_chunks(modules.len(), MoveModule::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::move_modules::table)
                .values(&modules[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_move_resources(
    conn: &mut PgConnection,
    wsc_details: &[WriteSetChangeDetail],
) -> Result<(), diesel::result::Error> {
    use schema::move_resources::dsl::*;

    let resources = wsc_details
        .iter()
        .filter_map(|detail| match detail {
            WriteSetChangeDetail::Resource(resource) => Some(resource.clone()),
            _ => None,
        })
        .collect::<Vec<MoveResource>>();

    let chunks = get_chunks(resources.len(), MoveResource::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::move_resources::table)
                .values(&resources[start_ind..end_ind])
                .on_conflict((transaction_version, write_set_change_index))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

/// This will insert all table data within each transaction within a block
fn insert_table_data(
    conn: &mut PgConnection,
    wsc_details: &[WriteSetChangeDetail],
) -> Result<(), diesel::result::Error> {
    use schema::{table_items::dsl as ti, table_metadatas::dsl as tm};

    let (items, metadata): (Vec<TableItem>, Vec<Option<TableMetadata>>) = wsc_details
        .iter()
        .filter_map(|detail| match detail {
            WriteSetChangeDetail::Table(table_item, table_metadata) => {
                Some((table_item.clone(), table_metadata.clone()))
            }
            _ => None,
        })
        .collect::<Vec<(TableItem, Option<TableMetadata>)>>()
        .into_iter()
        .unzip();
    let mut metadata_nonnull = metadata
        .iter()
        .filter_map(|x| x.clone())
        .collect::<Vec<TableMetadata>>();
    metadata_nonnull.dedup_by(|a, b| a.handle == b.handle);
    metadata_nonnull.sort_by(|a, b| a.handle.cmp(&b.handle));

    let chunks = get_chunks(items.len(), TableItem::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::table_items::table)
                .values(&items[start_ind..end_ind])
                .on_conflict((ti::transaction_version, ti::write_set_change_index))
                .do_nothing(),
            None,
        )?;
    }
    let chunks = get_chunks(metadata_nonnull.len(), TableMetadata::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::table_metadatas::table)
                .values(&metadata_nonnull[start_ind..end_ind])
                .on_conflict(tm::handle)
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}



fn insert_to_db_impl(
    conn: &mut PgConnection,
    coin_activities: &[CoinActivity],
    coin_infos: &[CoinInfo],
    coin_balances: &[CoinBalance],
    current_coin_balances: &[CurrentCoinBalance],
    coin_supply: &[CoinSupply],
    basic_token_transaction_lists: (&[Token], &[TokenOwnership], &[TokenData], &[CollectionData]), // token
    basic_token_current_lists: (
        &[CurrentTokenOwnership],
        &[CurrentTokenData],
        &[CurrentCollectionData],
    ),
    token_activities: &[TokenActivity],
    current_token_claims: &[CurrentTokenPendingClaim],
    current_ans_lookups: &[CurrentAnsLookup],
) -> Result<(), diesel::result::Error> {
    insert_coin_activities(conn, coin_activities)?;
    insert_coin_infos(conn, coin_infos)?;
    insert_coin_balances(conn, coin_balances)?;
    insert_current_coin_balances(conn, current_coin_balances)?;
    insert_coin_supply(conn, coin_supply)?;

    // token
    let (tokens, token_ownerships, token_datas, collection_datas) = basic_token_transaction_lists;
    let (current_token_ownerships, current_token_datas, current_collection_datas) =
        basic_token_current_lists;
    insert_tokens(conn, tokens)?;
    insert_token_datas(conn, token_datas)?;
    insert_token_ownerships(conn, token_ownerships)?;
    insert_collection_datas(conn, collection_datas)?;
    insert_current_token_ownerships(conn, current_token_ownerships)?;
    insert_current_token_datas(conn, current_token_datas)?;
    insert_current_collection_datas(conn, current_collection_datas)?;
    insert_token_activities(conn, token_activities)?;
    insert_current_token_claims(conn, current_token_claims)?;
    insert_current_ans_lookups(conn, current_ans_lookups)?;
    Ok(())
}

fn insert_coin_activities(
    conn: &mut PgConnection,
    item_to_insert: &[CoinActivity],
) -> Result<(), diesel::result::Error> {
    use schema::coin_activities::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinActivity::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_activities::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((
                    transaction_version,
                    event_account_address,
                    event_creation_number,
                    event_sequence_number,
                ))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_coin_infos(
    conn: &mut PgConnection,
    item_to_insert: &[CoinInfo],
) -> Result<(), diesel::result::Error> {
    use schema::coin_infos::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinInfo::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_infos::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict(coin_type_hash)
                .do_update()
                .set((
                    transaction_version_created.eq(excluded(transaction_version_created)),
                    creator_address.eq(excluded(creator_address)),
                    name.eq(excluded(name)),
                    symbol.eq(excluded(symbol)),
                    decimals.eq(excluded(decimals)),
                    transaction_created_timestamp.eq(excluded(transaction_created_timestamp)),
                    supply_aggregator_table_handle.eq(excluded(supply_aggregator_table_handle)),
                    supply_aggregator_table_key.eq(excluded(supply_aggregator_table_key)),
                )),
            Some(" WHERE coin_infos.transaction_version_created >= EXCLUDED.transaction_version_created "),
        )?;
    }
    Ok(())
}

fn insert_coin_balances(
    conn: &mut PgConnection,
    item_to_insert: &[CoinBalance],
) -> Result<(), diesel::result::Error> {
    use schema::coin_balances::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinBalance::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_balances::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, owner_address, coin_type_hash))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_current_coin_balances(
    conn: &mut PgConnection,
    item_to_insert: &[CurrentCoinBalance],
) -> Result<(), diesel::result::Error> {
    use schema::current_coin_balances::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CurrentCoinBalance::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_coin_balances::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((owner_address, coin_type_hash))
                .do_update()
                .set((
                    amount.eq(excluded(amount)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
                )),
            Some(" WHERE current_coin_balances.last_transaction_version <= excluded.last_transaction_version "),
        )?;
    }
    Ok(())
}

fn insert_coin_supply(
    conn: &mut PgConnection,
    item_to_insert: &[CoinSupply],
) -> Result<(), diesel::result::Error> {
    use schema::coin_supply::dsl::*;

    let chunks = get_chunks(item_to_insert.len(), CoinSupply::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::coin_supply::table)
                .values(&item_to_insert[start_ind..end_ind])
                .on_conflict((transaction_version, coin_type_hash))
                .do_update()
                .set((transaction_epoch.eq(excluded(transaction_epoch)),)),
            None,
        )?;
    }
    Ok(())
}


/// ============================ token ===============================
///

// fn insert_to_db_impl(
//     conn: &mut PgConnection,
//     basic_token_transaction_lists: (&[Token], &[TokenOwnership], &[TokenData], &[CollectionData]),
//     basic_token_current_lists: (
//         &[CurrentTokenOwnership],
//         &[CurrentTokenData],
//         &[CurrentCollectionData],
//     ),
//     token_activities: &[TokenActivity],
//     current_token_claims: &[CurrentTokenPendingClaim],
//     current_ans_lookups: &[CurrentAnsLookup],
// ) -> Result<(), diesel::result::Error> {
//     let (tokens, token_ownerships, token_datas, collection_datas) = basic_token_transaction_lists;
//     let (current_token_ownerships, current_token_datas, current_collection_datas) =
//         basic_token_current_lists;
//     insert_tokens(conn, tokens)?;
//     insert_token_datas(conn, token_datas)?;
//     insert_token_ownerships(conn, token_ownerships)?;
//     insert_collection_datas(conn, collection_datas)?;
//     insert_current_token_ownerships(conn, current_token_ownerships)?;
//     insert_current_token_datas(conn, current_token_datas)?;
//     insert_current_collection_datas(conn, current_collection_datas)?;
//     insert_token_activities(conn, token_activities)?;
//     insert_current_token_claims(conn, current_token_claims)?;
//     insert_current_ans_lookups(conn, current_ans_lookups)?;
//     Ok(())
// }

// fn insert_to_db(
//     conn: &mut PgPoolConnection,
//     name: &'static str,
//     start_version: u64,
//     end_version: u64,
//     basic_token_transaction_lists: (
//         Vec<Token>,
//         Vec<TokenOwnership>,
//         Vec<TokenData>,
//         Vec<CollectionData>,
//     ),
//     basic_token_current_lists: (
//         Vec<CurrentTokenOwnership>,
//         Vec<CurrentTokenData>,
//         Vec<CurrentCollectionData>,
//     ),
//     token_activities: Vec<TokenActivity>,
//     current_token_claims: Vec<CurrentTokenPendingClaim>,
//     current_ans_lookups: Vec<CurrentAnsLookup>,
// ) -> Result<(), diesel::result::Error> {
//     aptos_logger::trace!(
//         name = name,
//         start_version = start_version,
//         end_version = end_version,
//         "Inserting to db",
//     );
//     let (tokens, token_ownerships, token_datas, collection_datas) = basic_token_transaction_lists;
//     let (current_token_ownerships, current_token_datas, current_collection_datas) =
//         basic_token_current_lists;
//     match conn
//         .build_transaction()
//         .read_write()
//         .run::<_, Error, _>(|pg_conn| {
//             insert_to_db_impl(
//                 pg_conn,
//                 (&tokens, &token_ownerships, &token_datas, &collection_datas),
//                 (
//                     &current_token_ownerships,
//                     &current_token_datas,
//                     &current_collection_datas,
//                 ),
//                 &token_activities,
//                 &current_token_claims,
//                 &current_ans_lookups,
//
//             )
//         }) {
//         Ok(_) => Ok(()),
//         Err(_) => conn
//             .build_transaction()
//             .read_write()
//             .run::<_, Error, _>(|pg_conn| {
//                 let tokens = clean_data_for_db(tokens, true);
//                 let token_datas = clean_data_for_db(token_datas, true);
//                 let token_ownerships = clean_data_for_db(token_ownerships, true);
//                 let collection_datas = clean_data_for_db(collection_datas, true);
//                 let current_token_ownerships = clean_data_for_db(current_token_ownerships, true);
//                 let current_token_datas = clean_data_for_db(current_token_datas, true);
//                 let current_collection_datas = clean_data_for_db(current_collection_datas, true);
//                 let token_activities = clean_data_for_db(token_activities, true);
//                 let current_token_claims = clean_data_for_db(current_token_claims, true);
//                 let current_ans_lookups = clean_data_for_db(current_ans_lookups, true);
//
//                 insert_to_db_impl(
//                     pg_conn,
//                     (&tokens, &token_ownerships, &token_datas, &collection_datas),
//                     (
//                         &current_token_ownerships,
//                         &current_token_datas,
//                         &current_collection_datas,
//                     ),
//                     &token_activities,
//                     &current_token_claims,
//                     &current_ans_lookups,
//                 )
//             }),
//     }
// }

fn insert_tokens(
    conn: &mut PgConnection,
    tokens_to_insert: &[Token],
) -> Result<(), diesel::result::Error> {
    use schema::tokens::dsl::*;

    let chunks = get_chunks(tokens_to_insert.len(), Token::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::tokens::table)
                .values(&tokens_to_insert[start_ind..end_ind])
                .on_conflict((token_data_id_hash, property_version, transaction_version))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_token_ownerships(
    conn: &mut PgConnection,
    token_ownerships_to_insert: &[TokenOwnership],
) -> Result<(), diesel::result::Error> {
    use schema::token_ownerships::dsl::*;

    let chunks = get_chunks(
        token_ownerships_to_insert.len(),
        TokenOwnership::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_ownerships::table)
                .values(&token_ownerships_to_insert[start_ind..end_ind])
                .on_conflict((
                    token_data_id_hash,
                    property_version,
                    transaction_version,
                    table_handle,
                ))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_token_datas(
    conn: &mut PgConnection,
    token_datas_to_insert: &[TokenData],
) -> Result<(), diesel::result::Error> {
    use schema::token_datas::dsl::*;

    let chunks = get_chunks(token_datas_to_insert.len(), TokenData::field_count());
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_datas::table)
                .values(&token_datas_to_insert[start_ind..end_ind])
                .on_conflict((token_data_id_hash, transaction_version))
                .do_update()
                .set((description.eq(excluded(description)),)),
            None,
        )?;
    }
    Ok(())
}

fn insert_collection_datas(
    conn: &mut PgConnection,
    collection_datas_to_insert: &[CollectionData],
) -> Result<(), diesel::result::Error> {
    use schema::collection_datas::dsl::*;

    let chunks = get_chunks(
        collection_datas_to_insert.len(),
        CollectionData::field_count(),
    );
    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::collection_datas::table)
                .values(&collection_datas_to_insert[start_ind..end_ind])
                .on_conflict((collection_data_id_hash, transaction_version))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}

fn insert_current_token_ownerships(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentTokenOwnership],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_ownerships::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentTokenOwnership::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_ownerships::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((token_data_id_hash, property_version, owner_address))
                .do_update()
                .set((
                    creator_address.eq(excluded(creator_address)),
                    collection_name.eq(excluded(collection_name)),
                    name.eq(excluded(name)),
                    amount.eq(excluded(amount)),
                    token_properties.eq(excluded(token_properties)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    collection_data_id_hash.eq(excluded(collection_data_id_hash)),
                    table_type.eq(excluded(table_type)),
                )),
            Some(" WHERE current_token_ownerships.last_transaction_version <= excluded.last_transaction_version "),
        )?;
    }
    Ok(())
}

fn insert_current_token_datas(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentTokenData],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_datas::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentTokenData::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_datas::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(token_data_id_hash)
                .do_update()
                .set((
                    creator_address.eq(excluded(creator_address)),
                    collection_name.eq(excluded(collection_name)),
                    name.eq(excluded(name)),
                    maximum.eq(excluded(maximum)),
                    supply.eq(excluded(supply)),
                    largest_property_version.eq(excluded(largest_property_version)),
                    metadata_uri.eq(excluded(metadata_uri)),
                    payee_address.eq(excluded(payee_address)),
                    royalty_points_numerator.eq(excluded(royalty_points_numerator)),
                    royalty_points_denominator.eq(excluded(royalty_points_denominator)),
                    maximum_mutable.eq(excluded(maximum_mutable)),
                    uri_mutable.eq(excluded(uri_mutable)),
                    description_mutable.eq(excluded(description_mutable)),
                    properties_mutable.eq(excluded(properties_mutable)),
                    royalty_mutable.eq(excluded(royalty_mutable)),
                    default_properties.eq(excluded(default_properties)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    collection_data_id_hash.eq(excluded(collection_data_id_hash)),
                    description.eq(excluded(description)),
                )),
            Some(" WHERE current_token_datas.last_transaction_version <= excluded.last_transaction_version "),
        )?;
    }
    Ok(())
}

fn insert_current_collection_datas(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentCollectionData],
) -> Result<(), diesel::result::Error> {
    use schema::current_collection_datas::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentCollectionData::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_collection_datas::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict(collection_data_id_hash)
                .do_update()
                .set((
                    creator_address.eq(excluded(creator_address)),
                    collection_name.eq(excluded(collection_name)),
                    description.eq(excluded(description)),
                    metadata_uri.eq(excluded(metadata_uri)),
                    supply.eq(excluded(supply)),
                    maximum.eq(excluded(maximum)),
                    maximum_mutable.eq(excluded(maximum_mutable)),
                    uri_mutable.eq(excluded(uri_mutable)),
                    description_mutable.eq(excluded(description_mutable)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                    table_handle.eq(excluded(table_handle)),
                )),
            Some(" WHERE current_collection_datas.last_transaction_version <= excluded.last_transaction_version "),
        )?;
    }
    Ok(())
}

fn insert_token_activities(
    conn: &mut PgConnection,
    items_to_insert: &[TokenActivity],
) -> Result<(), diesel::result::Error> {
    use schema::token_activities::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), TokenActivity::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::token_activities::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((
                    transaction_version,
                    event_account_address,
                    event_creation_number,
                    event_sequence_number,
                ))
                .do_nothing(),
            None,
        )?;
    }
    Ok(())
}
fn insert_current_token_claims(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentTokenPendingClaim],
) -> Result<(), diesel::result::Error> {
    use schema::current_token_pending_claims::dsl::*;

    let chunks = get_chunks(
        items_to_insert.len(),
        CurrentTokenPendingClaim::field_count(),
    );

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_token_pending_claims::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((
                    token_data_id_hash, property_version, from_address, to_address
                ))
                .do_update()
                .set((
                    collection_data_id_hash.eq(excluded(collection_data_id_hash)),
                    creator_address.eq(excluded(creator_address)),
                    collection_name.eq(excluded(collection_name)),
                    name.eq(excluded(name)),
                    amount.eq(excluded(amount)),
                    table_handle.eq(excluded(table_handle)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
            Some(" WHERE current_token_pending_claims.last_transaction_version <= excluded.last_transaction_version "),
        )?;
    }
    Ok(())
}

fn insert_current_ans_lookups(
    conn: &mut PgConnection,
    items_to_insert: &[CurrentAnsLookup],
) -> Result<(), diesel::result::Error> {
    use schema::current_ans_lookup::dsl::*;

    let chunks = get_chunks(items_to_insert.len(), CurrentAnsLookup::field_count());

    for (start_ind, end_ind) in chunks {
        execute_with_better_error(
            conn,
            diesel::insert_into(schema::current_ans_lookup::table)
                .values(&items_to_insert[start_ind..end_ind])
                .on_conflict((domain, subdomain))
                .do_update()
                .set((
                    registered_address.eq(excluded(registered_address)),
                    expiration_timestamp.eq(excluded(expiration_timestamp)),
                    last_transaction_version.eq(excluded(last_transaction_version)),
                )),
            Some(" WHERE current_ans_lookup.last_transaction_version <= excluded.last_transaction_version "),
        )?;
    }
    Ok(())
}



#[async_trait]
impl TransactionProcessor for AllTransactionProcessor {
    fn name(&self) -> &'static str {
        NAME
    }

    async fn process_transactions(
        &self,
        transactions: Vec<Transaction>,
        start_version: u64,
        end_version: u64,
    ) -> Result<ProcessingResult, TransactionProcessingError> {
        let (txns, user_txns, bm_txns, events, write_set_changes) =
            TransactionModel::from_transactions(&transactions);

        let mut conn = self.get_conn();

        let maybe_aptos_coin_info =
            &CoinInfoQuery::get_by_coin_type(APTOS_COIN_TYPE.to_string(), &mut conn).unwrap();

        let mut all_coin_activities = vec![];
        let mut all_coin_balances = vec![];
        let mut all_coin_infos: HashMap<String, CoinInfo> = HashMap::new();
        let mut all_current_coin_balances: HashMap<CurrentCoinBalancePK, CurrentCoinBalance> =
            HashMap::new();
        let mut all_coin_supply = vec![];

        let mut all_tokens = vec![];
        let mut all_token_ownerships = vec![];
        let mut all_token_datas = vec![];
        let mut all_collection_datas = vec![];
        let mut all_token_activities = vec![];

        // Hashmap key will be the PK of the table, we do not want to send duplicates writes to the db within a batch
        let mut all_current_token_ownerships: HashMap<
            CurrentTokenOwnershipPK,
            CurrentTokenOwnership,
        > = HashMap::new();
        let mut all_current_token_datas: HashMap<TokenDataIdHash, CurrentTokenData> =
            HashMap::new();
        let mut all_current_collection_datas: HashMap<TokenDataIdHash, CurrentCollectionData> =
            HashMap::new();
        let mut all_current_token_claims: HashMap<
            CurrentTokenPendingClaimPK,
            CurrentTokenPendingClaim,
        > = HashMap::new();
        let mut all_current_ans_lookups: HashMap<CurrentAnsLookupPK, CurrentAnsLookup> =
            HashMap::new();

        for txn in &transactions {
            // coin
            let (
                mut coin_activities,
                mut coin_balances,
                coin_infos,
                current_coin_balances,
                mut coin_supply,
            ) = CoinActivity::from_transaction(txn, maybe_aptos_coin_info);
            all_coin_activities.append(&mut coin_activities);
            all_coin_balances.append(&mut coin_balances);
            all_coin_supply.append(&mut coin_supply);
            // For coin infos, we only want to keep the first version, so insert only if key is not present already
            for (key, value) in coin_infos {
                all_coin_infos.entry(key).or_insert(value);
            }
            all_current_coin_balances.extend(current_coin_balances);

            // token
            let (
                mut tokens,
                mut token_ownerships,
                mut token_datas,
                mut collection_datas,
                current_token_ownerships,
                current_token_datas,
                current_collection_datas,
                current_token_claims,
            ) = Token::from_transaction(&txn, &mut conn);
            all_tokens.append(&mut tokens);
            all_token_ownerships.append(&mut token_ownerships);
            all_token_datas.append(&mut token_datas);
            all_collection_datas.append(&mut collection_datas);
            // Given versions will always be increasing here (within a single batch), we can just override current values
            all_current_token_ownerships.extend(current_token_ownerships);
            all_current_token_datas.extend(current_token_datas);
            all_current_collection_datas.extend(current_collection_datas);

            // Track token activities
            let mut activities = TokenActivity::from_transaction(&txn);
            all_token_activities.append(&mut activities);

            // claims
            all_current_token_claims.extend(current_token_claims);

            // ANS lookups
            let current_ans_lookups =
                CurrentAnsLookup::from_transaction(&txn, self.ans_contract_address.clone());
            all_current_ans_lookups.extend(current_ans_lookups);
        }
        let mut all_coin_infos = all_coin_infos.into_values().collect::<Vec<CoinInfo>>();
        let mut all_current_coin_balances = all_current_coin_balances
            .into_values()
            .collect::<Vec<CurrentCoinBalance>>();

        // Sort by PK
        all_coin_infos.sort_by(|a, b| a.coin_type.cmp(&b.coin_type));
        all_current_coin_balances.sort_by(|a, b| {
            (&a.owner_address, &a.coin_type).cmp(&(&b.owner_address, &b.coin_type))
        });

        // token
        // Getting list of values and sorting by pk in order to avoid postgres deadlock since we're doing multi threaded db writes
        let mut all_current_token_ownerships = all_current_token_ownerships
            .into_values()
            .collect::<Vec<CurrentTokenOwnership>>();
        let mut all_current_token_datas = all_current_token_datas
            .into_values()
            .collect::<Vec<CurrentTokenData>>();
        let mut all_current_collection_datas = all_current_collection_datas
            .into_values()
            .collect::<Vec<CurrentCollectionData>>();
        let mut all_current_token_claims = all_current_token_claims
            .into_values()
            .collect::<Vec<CurrentTokenPendingClaim>>();

        // Sort by PK
        all_current_token_ownerships.sort_by(|a, b| {
            (&a.token_data_id_hash, &a.property_version, &a.owner_address).cmp(&(
                &b.token_data_id_hash,
                &b.property_version,
                &b.owner_address,
            ))
        });
        all_current_token_datas.sort_by(|a, b| a.token_data_id_hash.cmp(&b.token_data_id_hash));
        all_current_collection_datas
            .sort_by(|a, b| a.collection_data_id_hash.cmp(&b.collection_data_id_hash));
        all_current_token_claims.sort_by(|a, b| {
            (
                &a.token_data_id_hash,
                &a.property_version,
                &a.from_address,
                &a.to_address,
            )
                .cmp(&(
                    &b.token_data_id_hash,
                    &b.property_version,
                    &b.from_address,
                    &a.to_address,
                ))
        });
        // Sort ans lookup values for postgres insert
        let mut all_current_ans_lookups = all_current_ans_lookups
            .into_values()
            .collect::<Vec<CurrentAnsLookup>>();
        all_current_ans_lookups
            .sort_by(|a, b| a.domain.cmp(&b.domain).then(a.subdomain.cmp(&b.subdomain)));

        let tx_result = insert_to_db(
            &mut conn,
            self.name(),
            start_version,
            end_version,
            txns,
            user_txns,
            bm_txns,
            events,
            write_set_changes,
            all_coin_activities,
            all_coin_infos,
            all_coin_balances,
            all_current_coin_balances,
            all_coin_supply,
            (
                all_tokens,
                all_token_ownerships,
                all_token_datas,
                all_collection_datas,
            ),
            (
                all_current_token_ownerships,
                all_current_token_datas,
                all_current_collection_datas,
            ),
            all_token_activities,
            all_current_token_claims,
            all_current_ans_lookups,
        );

        match tx_result {
            Ok(_) => Ok(ProcessingResult::new(
                self.name(),
                start_version,
                end_version,
            )),
            Err(err) => Err(TransactionProcessingError::TransactionCommitError((
                anyhow::Error::from(err),
                start_version,
                end_version,
                self.name(),
            ))),
        }
    }

    fn connection_pool(&self) -> &PgDbPool {
        &self.connection_pool
    }
}
