var connector = require('../node_modules/aquaria/common/connector');

const prePopulateRegions = async function(suffix = '') {
  const sqlQuery = {}
  sqlQuery.sql =
    "DROP TABLE IF EXISTS `region" + suffix + "`; " +
    "CREATE TABLE `region" + suffix +
    "` ( \
    `Region_Type` VARCHAR(20) NOT NULL, \
    `Accession_Id` INT(11) UNSIGNED NOT NULL, \
    `Start_Residue` INT(11) UNSIGNED NOT NULL DEFAULT '0', \
    `End_Residue` INT(11) UNSIGNED NOT NULL DEFAULT '0', \
    PRIMARY KEY (`Region_Type`,`Accession_Id`,`Start_Residue`,`End_Residue`) \
  ) ENGINE=INNODB DEFAULT CHARSET=utf8 \
  /*!50500 PARTITION BY LIST  COLUMNS(Region_Type) \
  (PARTITION PSSH2_Dark_Region_Partition VALUES IN ('pssh2_dark') ENGINE = InnoDB, \
  PARTITION PSSH2_Non_Dark_Region_Partition VALUES IN ('pssh2_non_dark') ENGINE = InnoDB, \
  PARTITION Uniprot_Non_Dark_Region_Partition VALUES IN ('uniprot_non_dark') ENGINE = InnoDB, \
  PARTITION Total_Non_Dark_Region_Partition VALUES IN ('total_non_dark') ENGINE = InnoDB, \
  PARTITION Total_Dark_Region_Partition VALUES IN ('total_dark') ENGINE = InnoDB) */; " +
    "INSERT INTO `region" +
    suffix +
    "` \
  SELECT DISTINCT 'uniprot_non_dark' ,a.id Accession_Id,ifnull(Start_Residue,0),ifnull(End_Residue,0) \
  FROM aquaria.Uniprot_PDB u \
  INNER JOIN aquaria.protein_sequence p ON u.primary_accession = p.primary_accession \
  INNER JOIN accession a ON p.md5_hash = a.md5_hash; " +
    "INSERT INTO `region" +
    suffix +
    "`  \
  SELECT DISTINCT 'pssh2_non_dark' ,Accession_Id,ifnull(Start_Residue,0),ifnull(End_Residue,0) \
  FROM `non_dark_region" + suffix + "`;"

  return connector.queryPromise(sqlQuery.sql, sqlQuery.args);
}

const populateRegionWithNonDark = async function(suffix = '') {

  const sqlQuery = {}
  sqlQuery.sql =
    "SET @interval_id = 0; " +
    "SET @interval_end = 0; " +
    "SET @current_sequence_id =''; " +
    "SET @current_accession_id =0; " +
    "DROP TABLE IF EXISTS `total_non_dark_region" +
    suffix + "`; " +
    "CREATE TABLE `total_non_dark_region" + suffix +
    "` AS \
SELECT \
Accession_Id, \
  CASE WHEN MAX(interval_end) > 0 AND MIN(start_residue) = 0 THEN 1 ELSE MIN(start_residue) END AS start_residue, \
  MAX(interval_end) AS end_residue, \
  CASE WHEN MAX(interval_end) = MIN(start_residue) THEN 0 ELSE MAX(interval_end)-CASE WHEN MAX(interval_end) > 0 AND MIN(start_residue) = 0 THEN 1 ELSE MIN(start_residue) END+1 END AS interval_length \
  FROM \
    ( \
SELECT " +
    "@interval_id:= IF(@current_accession_id = Accession_Id, IF(start_residue > @interval_end, @interval_id + 1, @interval_id), 1) AS interval_id, \
    @interval_end:= IF(@current_accession_id = Accession_Id, IF(start_residue < @interval_end, GREATEST(@interval_end, end_residue), end_residue), end_residue) AS interval_end, \
    @current_accession_id:= Accession_Id current_accession_id, \
    u.* \
    FROM `region" +
    suffix + "` u \
  ORDER BY Accession_Id, start_residue, end_residue) tmp \
GROUP BY Accession_Id, interval_id \
ORDER BY Accession_Id, interval_id; " +
    "INSERT INTO `region" + suffix +
    "` SELECT DISTINCT 'total_non_dark', Accession_Id, start_residue, end_residue \
FROM `total_non_dark_region" + suffix + "`;"

  return connector.queryPromise(sqlQuery.sql, sqlQuery.args);
}


const populateNonDarkInterval = async function(suffix = '') {

  const sqlQuery = {}
  sqlQuery.sql =
    "DROP TABLE IF EXISTS `non_dark_interval" + suffix +
    "`;" +
    "CREATE TABLE `non_dark_interval" + suffix +
    "` AS \
SELECT Accession_Id, CASE WHEN start_residue = 0 THEN 0 ELSE cast(sum(end_residue - start_residue +1) AS UNSIGNED) END interval_length,cast(1 AS DECIMAL(8,7)) darkness \
FROM `region" +
    suffix +
    "`  \
WHERE region_type = 'total_non_dark' \
GROUP BY Accession_Id; " +
    "CREATE INDEX non_dark_interval_accession_id ON `non_dark_interval" +
    suffix +
    "` (Accession_Id); \
UPDATE  `non_dark_interval" +
    suffix +
    "` i \
INNER JOIN accession a ON i.Accession_Id  = a.id \
SET  i.interval_length = a.sequence_length \
WHERE interval_length > sequence_length; " +
    "UPDATE `non_dark_interval" +
    suffix +
    "` \
SET darkness = cast(((SELECT sequence_length FROM accession WHERE `non_dark_interval" +
    suffix +
    "`.Accession_Id = accession.id)-interval_length)/(SELECT sequence_length*1.0 FROM accession WHERE `non_dark_interval" +
    suffix +
    "`.Accession_Id = accession.id) AS DECIMAL(8,7)); "

  return connector.queryPromise(sqlQuery.sql, sqlQuery.args);

}

const populateDomainResults = async function(suffix = '') {
  const sqlQuery = {}
  sqlQuery.sql =
    "DROP TABLE IF EXISTS `domain_results" + suffix +
    "`; \
CREATE TABLE `domain_results" + suffix +
    "` AS \
SELECT a.primary_accession,s.primary_accession accession,s.domain,a.sequence_length,ifnull(i.interval_length,0)interval_length,ifnull(i.darkness,1) darkness,CASE WHEN EXISTS (SELECT 1 FROM region r WHERE r.Accession_Id = a.id AND r.region_type = 'Uniprot_Non_Dark') THEN 1 ELSE 0 END Is_Unprot , case when ifnull(i.interval_length,0) = a.sequence_length then 1 else 0 end Is_White \
FROM accession a  \
INNER JOIN aquaria.protein_sequence s ON a.md5_hash = s.md5_hash AND source_database = 'swissprot' AND a.primary_accession = s.primary_accession \
LEFT JOIN `non_dark_interval" +
    suffix +
    "` i ON a.id = i.Accession_Id; \
DROP VIEW IF EXISTS `Domain_Results_Summary" +
    suffix + "`; \
CREATE VIEW `Domain_Results_Summary" + suffix +
    "` \
AS SELECT \
   ifnull(`domain`,'ALL') AS `Domain`,count(0) AS `proteins`,sum(`domain_results`.`sequence_length`) AS `total_residues`,sum(`domain_results`.`interval_length`) AS `total_non_dark_residues`,(sum(`domain_results`.`sequence_length`) - sum(`domain_results`.`interval_length`)) AS `total_dark_residues`,sum((case when (`domain_results`.`interval_length` = 0) then `domain_results`.`sequence_length` else NULL end)) AS `black_residues`,sum((case when ((`domain_results`.`interval_length` = 0) or (`domain_results`.`Is_White` = 1)) then NULL else (`domain_results`.`sequence_length` - `domain_results`.`interval_length`) end)) AS `dark_residues`,sum((case when ((`domain_results`.`interval_length` = 0) or (`domain_results`.`Is_White` = 1)) then NULL else `domain_results`.`interval_length` end)) AS `grey_residues`,sum((case when (`domain_results`.`Is_White` = 1) then `domain_results`.`sequence_length` else NULL end)) AS `white_residues`,((sum(`domain_results`.`sequence_length`) - sum(`domain_results`.`interval_length`)) / sum(`domain_results`.`sequence_length`)) AS `total_percent_dark`,avg(`domain_results`.`darkness`) AS `average_percent_dark`,(sum((case when (`domain_results`.`interval_length` = 0) then `domain_results`.`sequence_length` else NULL end)) / sum(`domain_results`.`sequence_length`)) AS `Dark`,(sum((case when ((`domain_results`.`interval_length` = 0) or (`domain_results`.`Is_White` = 1)) then NULL else (`domain_results`.`sequence_length` - `domain_results`.`interval_length`) end)) / sum(`domain_results`.`sequence_length`)) AS `dark_region`,(sum((case when ((`domain_results`.`interval_length` = 0) or (`domain_results`.`Is_White` = 1)) then NULL else `domain_results`.`interval_length` end)) / sum(`domain_results`.`sequence_length`)) AS `grey_region`,(sum((case when (`domain_results`.`Is_White` = 1) then `domain_results`.`sequence_length` else NULL end)) / sum(`domain_results`.`sequence_length`)) AS `White` \
FROM `domain_results" +
    suffix +
    "` `domain_results` group by `domain_results`.`domain` with rollup; "
  return connector.queryPromise(sqlQuery.sql, sqlQuery.args);
}

const populateDomainResultsDetails = function(suffix = '') {
  const sqlQuery = {}
  sqlQuery.sql =
    "SET SESSION group_concat_max_len = 1000000; \
DROP TABLE IF EXISTS `Domain_results_details" +
    suffix + "`; \
CREATE TABLE `Domain_results_details" + suffix +
    "` AS \
SELECT r.accession,r.domain, r.sequence_length,r.interval_length,r.darkness ,GROUP_CONCAT(concat(n.start_residue,'-',n.end_residue) SEPARATOR ', ') non_dark_regions \
FROM `domain_results" +
    suffix +
    "` r \
INNER JOIN aquaria.protein_sequence s ON r.primary_accession = s.primary_accession \
INNER JOIN accession a ON s.md5_hash = a.md5_hash \
INNER JOIN `region" +
    suffix +
    "` n ON a.id = n.Accession_Id AND region_type  = 'total_non_dark' \
GROUP BY r.accession,r.domain, r.sequence_length,r.interval_length,r.darkness;"
  return connector.queryPromise(sqlQuery.sql, sqlQuery.args);
}

exports.start_post_processing = async function(suffix) {
  await prePopulateRegions(suffix)
  await populateRegionWithNonDark(suffix)
  await populateNonDarkInterval(suffix)
  await populateDomainResults(suffix)
  await populateDomainResultsDetails(suffix)
  console.log("finished " + suffix + " run")
}