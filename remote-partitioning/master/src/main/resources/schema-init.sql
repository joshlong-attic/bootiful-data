SET foreign_key_checks = 0;
drop table BATCH_JOB_EXECUTION  ;
drop table BATCH_JOB_EXECUTION_CONTEXT ;
drop table BATCH_JOB_EXECUTION_PARAMS ;
drop table BATCH_JOB_EXECUTION_SEQ ;
drop table BATCH_JOB_INSTANCE ;
drop table BATCH_JOB_SEQ ;
drop table BATCH_STEP_EXECUTION ;
drop table BATCH_STEP_EXECUTION_CONTEXT ;
drop table BATCH_STEP_EXECUTION_SEQ;
SET foreign_key_checks = 1;

-- CUSTOMERS
DROP TABLE IF EXISTS `CUSTOMER`;
DROP TABLE IF EXISTS `NEW_CUSTOMER`;

CREATE TABLE `CUSTOMER` (
  `id` mediumint(8) unsigned NOT NULL auto_increment,
  `firstName` varchar(255) default NULL,
  `lastName` varchar(255) default NULL,
  `birthdate` varchar(255),
  PRIMARY KEY (`id`)
) AUTO_INCREMENT=1;

CREATE TABLE `NEW_CUSTOMER` (
  `id` mediumint(8) unsigned NOT NULL auto_increment,
  `firstName` varchar(255) default NULL,
  `lastName` varchar(255) default NULL,
  `birthdate` varchar(255),
  PRIMARY KEY (`id`)
) AUTO_INCREMENT=1;


