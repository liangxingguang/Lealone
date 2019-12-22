/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.server.protocol;

public enum PacketType {

    // 命令值会包含在协议包中不能随便改动，不同类型的命令值之间有意设置了间隔，用于后续加新命令

    SESSION_INIT(0),
    SESSION_CANCEL_STATEMENT(1),
    SESSION_SET_AUTO_COMMIT(2),
    SESSION_CLOSE(3),
    SESSION_INIT_ACK(4),

    RESULT_FETCH_ROWS(20),
    RESULT_FETCH_ROWS_ACK(20),
    RESULT_CHANGE_ID(21),
    RESULT_RESET(22),
    RESULT_CLOSE(23),

    COMMAND_QUERY(40),
    COMMAND_UPDATE(41),
    COMMAND_QUERY_ACK(42),
    COMMAND_UPDATE_ACK(43),

    COMMAND_PREPARE(50),
    COMMAND_PREPARE_READ_PARAMS(51),
    COMMAND_PREPARED_QUERY(52),
    COMMAND_PREPARED_UPDATE(53),

    COMMAND_PREPARE_ACK(54),
    COMMAND_PREPARE_READ_PARAMS_ACK(55),
    COMMAND_PREPARED_QUERY_ACK(56),
    COMMAND_PREPARED_UPDATE_ACK(57),

    COMMAND_GET_META_DATA(70),
    COMMAND_GET_META_DATA_ACK(71),
    COMMAND_READ_LOB(72),
    COMMAND_READ_LOB_ACK(73),
    COMMAND_CLOSE(74),

    COMMAND_REPLICATION_UPDATE(80),
    COMMAND_REPLICATION_UPDATE_ACK(81),
    COMMAND_REPLICATION_PREPARED_UPDATE(82),
    COMMAND_REPLICATION_PREPARED_UPDATE_ACK(83),
    COMMAND_REPLICATION_COMMIT(84),
    COMMAND_REPLICATION_ROLLBACK(85),
    COMMAND_REPLICATION_CHECK_CONFLICT(86),
    COMMAND_REPLICATION_CHECK_CONFLICT_ACK(87),
    COMMAND_REPLICATION_HANDLE_CONFLICT(88),

    COMMAND_DISTRIBUTED_TRANSACTION_QUERY(100),
    COMMAND_DISTRIBUTED_TRANSACTION_QUERY_ACK(101),
    COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY(102),
    COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_QUERY_ACK(103),
    COMMAND_DISTRIBUTED_TRANSACTION_UPDATE(104),
    COMMAND_DISTRIBUTED_TRANSACTION_UPDATE_ACK(105),
    COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE(106),
    COMMAND_DISTRIBUTED_TRANSACTION_PREPARED_UPDATE_ACK(107),

    COMMAND_DISTRIBUTED_TRANSACTION_COMMIT(120),
    COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK(121),
    COMMAND_DISTRIBUTED_TRANSACTION_ADD_SAVEPOINT(122),
    COMMAND_DISTRIBUTED_TRANSACTION_ROLLBACK_SAVEPOINT(123),
    COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE(124),
    COMMAND_DISTRIBUTED_TRANSACTION_VALIDATE_ACK(125),

    COMMAND_BATCH_STATEMENT_UPDATE(140),
    COMMAND_BATCH_STATEMENT_PREPARED_UPDATE(141),

    COMMAND_STORAGE_PUT(160),
    COMMAND_STORAGE_PUT_ACK(161),
    COMMAND_STORAGE_APPEND(162),
    COMMAND_STORAGE_APPEND_ACK(163),
    COMMAND_STORAGE_GET(164),
    COMMAND_STORAGE_GET_ACK(165),

    COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE(180),
    COMMAND_STORAGE_PREPARE_MOVE_LEAF_PAGE_ACK(181),
    COMMAND_STORAGE_MOVE_LEAF_PAGE(182),
    COMMAND_STORAGE_REMOVE_LEAF_PAGE(183),
    COMMAND_STORAGE_REPLICATE_ROOT_PAGES(184),
    COMMAND_STORAGE_READ_PAGE(185),
    COMMAND_STORAGE_READ_PAGE_ACK(186),

    COMMAND_P2P_MESSAGE(300),
    P2P_ECHO(301),
    P2P_GOSSIP_DIGEST_SYN(302),
    P2P_GOSSIP_DIGEST_ACK(303),
    P2P_GOSSIP_DIGEST_ACK2(304),
    P2P_GOSSIP_SHUTDOWN(305),
    P2P_REQUEST_RESPONSE(306),

    VOID(400),

    STATUS_OK(1000),
    STATUS_CLOSED(1001),
    STATUS_ERROR(1002),
    STATUS_RUN_MODE_CHANGED(1003);

    public final int value;

    private PacketType(int value) {
        this.value = value;
    }
}
