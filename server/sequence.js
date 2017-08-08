
var Sequence = require('../node_modules/aquaria/shared/sequence');
var Errors = require('../node_modules/aquaria/shared/Errors');
var Promise = require('es6-promise').Promise;

exports.get_uniprot_sequence= function(data, matches) {
  return new Promise(function(resolve, reject) {
    'use strict';
    matches.uniprot_primary_accession = [];
    matches.uniprot_sequence = [];
    matches.uniprot_sequence_length = [];
    matches.uniprot_sequence_description = [];
    matches.uniprot_sequence_MD5_Hash = [];
    var sequences = data.map(function(sequenceData) {
      return new Sequence(sequenceData);
    });

    sequences.forEach(function(sequence) {
      matches.uniprot_primary_accession.push(sequence.primary_accession);
      matches.uniprot_sequence.push(sequence.sequence);
      matches.uniprot_sequence_length.push(sequence.sequence.length);
      matches.uniprot_sequence_description.push(sequence.description);
      matches.uniprot_sequence_MD5_Hash.push(sequence.uniprot_hash);
    });
    if (sequences.length > 0) {
      resolve(sequences);
    } else {
      reject(new Errors.MatchingStructuresError("No sequences found."));
    }
  });
};