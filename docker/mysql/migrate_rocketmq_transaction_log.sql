CREATE TABLE IF NOT EXISTS `tb_mq_transaction_log` (
  `transaction_id` varchar(64) NOT NULL COMMENT 'RocketMQ事务ID',
  `business_type` varchar(32) NOT NULL COMMENT '业务类型',
  `business_id` varchar(64) NOT NULL COMMENT '业务主键',
  `status` varchar(16) NOT NULL COMMENT 'COMMITTED',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`transaction_id`),
  KEY `idx_mq_tx_business` (`business_type`,`business_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='RocketMQ本地事务日志';
