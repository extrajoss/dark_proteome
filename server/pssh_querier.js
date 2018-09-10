var logger = require('../node_modules/aquaria/common/log');
var Errors = require('../node_modules/aquaria/shared/Errors');
var connector = require('../node_modules/aquaria/common/connector');

module.exports.getPSSHAndPDBRowsPromise = function(sequence, rowCallback,options) {

  var psshRows = null;
  return connector.queryPromise(getPSSHSQL(), [ sequence.uniprot_hash ],options).then(
      function(results) {
        psshRows = results;
        if (results.length == 0) {
          throw Errors.MatchingStructures('No alignments found.'); 
        }
        var chainHashes = results.map(function(psshRow) {
          return psshRow.PDB_chain_hash;
        });
        return connector.queryBatchList(chainHashes, getPDBSQL);

      }).then(function(pdbChainRows) {
    var pdbChainRowMap = {};
    pdbChainRows.forEach(function(pdbChainRow) {
      var existingList = pdbChainRowMap[pdbChainRow.MD5_Hash];
      if (existingList) {
        existingList.push(pdbChainRow);
      } else {
        pdbChainRowMap[pdbChainRow.MD5_Hash] = [ pdbChainRow ];
      }
    });
    return pdbChainRowMap;
  }).then(function(pdbChainRowMap) {
    psshRows.forEach(function(psshRow) {
      var chainRows = pdbChainRowMap[psshRow.PDB_chain_hash];
      if (typeof chainRows === 'undefined') {
        console.log("couldn't find chainRows for : " + psshRow.PDB_chain_hash);
      } else {
        chainRows.forEach(function(chainRow) {
          rowCallback(psshRow, chainRow);
        });
      }
    });
  });
}

var getPSSHSQL = function() {
  return "select psshResolved.PDB_chain_hash, Match_length, E_value, Identity_Score, Repeat_domains, Alignment from PSSH2.pssh2_DarkProtein as psshResolved where protein_sequence_hash = ? \
 ORDER BY Identity_Score DESC, Match_length DESC";
};

var getPDBSQL = function(chainHashes) {
  var q = chainHashes.map(function() {
    return '?'
  });
  // q.length = chainHashes.length;
  return "select \
PDB_ID, Chain, Model, PDB_chain.MD5_Hash, Matches, Align_to_SEQRES from aquaria.PDB_chain where PDB_chain.MD5_Hash  in ("
      + q.join(",") + ") and Model = 1;";
};
