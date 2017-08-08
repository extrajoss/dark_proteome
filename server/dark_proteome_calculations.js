var connector = require('../node_modules/aquaria/common/connector');
var logger = require('../node_modules/aquaria/common/log');
var Promise = require('es6-promise').Promise;
var matching_structures = require('../node_modules/aquaria/aquaria/matching_structures');
var PSSHProcessor = require('./psshProcessor');
var matching_structures_querier = require('../node_modules/aquaria/aquaria/matching_structures_querier');
var Errors = require('../node_modules/aquaria/shared/Errors');
var sequence = require('./sequence');

exports.check_progress = function(){
    return connector.queryPromise("select count(distinct accession_id)progress from non_dark_region;", [])
     .then(function(data){
         return data;
     });
}

exports.get_dark_regions = function(request, response, next){
  return new Promise (function (resolve, reject) {
    var id = RegExp.$1;
    logger.info('the id for Dark Regions JSON is: ' + id);
    try {
      var loadRequest = {
          selector: [id]
      };        
      var uniprot_primary_accession = loadRequest.selector[0];
      get_dark_regions_from_accession(
        uniprot_primary_accession, 
        function (accession, regions) {
          var data = {
            regions: regions
          };
          resolve(data);
        });
    }
    catch (err) {
      console.log("ERR caught writing JSON for: " + id + ", err: " + err);
      response.end("{}");
    }    
  });
};

var save_remaining_dark_regions = function(){
    var sqlQuery = {};
    sqlQuery.sql = "SELECT Id, Primary_Accession FROM accession WHERE Id NOT IN (SELECT Accession_Id FROM dark_region) AND Id NOT IN (SELECT Accession_Id FROM non_dark_region) limit 50";
    sqlQuery.args = [10];
    return connector.queryPromise(sqlQuery.sql, sqlQuery.args)
        .then(function(accessions){
            return accessions.map(write_non_dark_regions_for);
        }).then(function(accessionPromises){
            if(accessionPromises.length==0){
                return;
            }
            return Promise.all(accessionPromises)
                .then(function(results){
                    save_remaining_dark_regions();
                });
        }).catch(function(err){
            console.log(err);
        });
    }
         
    
var write_non_dark_regions_for = function(accession){
    return get_non_dark_regions(accession)
        .then(function(results){
            console.log(results);
            return write_regions(results.accession,results.regions);
        });
}

var write_regions =function(accession,regions){
        var accession_id = accession.Id;
        var queryPromises =[];
        if (!accession_id){
            console.log(accession);
        }
        var n_regions = 0;
        if (regions.dark_regions){
            n_regions = regions.dark_regions.length;
            var dark_regions = [];
            for(region_index=0;region_index<n_regions;region_index++){
                var dark_region = regions.dark_regions[region_index];
                if (dark_region && dark_region[0] && dark_region[1]){
                    dark_regions.push([accession_id, dark_region[0], dark_region[1]]);
                }
            }
            var dark_sql = "insert into dark_region(Accession_Id,Start_residue,End_Residue) VALUES ?"
            if (dark_regions.length==0){  
                    dark_regions.push([accession_id, null, null]);
            }  
            queryPromises.push(connector.queryPromise(dark_sql, [dark_regions])); 
        }

        n_regions = 0;
        if (regions.non_dark_regions){
            n_regions = regions.non_dark_regions.length;
            var non_dark_regions = [];
            for(region_index=0;region_index<n_regions;region_index++){
                var non_dark_region = regions.non_dark_regions[region_index];
                if (non_dark_region && non_dark_region[0] && non_dark_region[1]){
                    non_dark_regions.push([accession_id, non_dark_region[0], non_dark_region[1]]);
                }
            }

            var non_dark_sql = "insert into non_dark_region(Accession_Id,Start_residue,End_Residue) VALUES ?"
            if (non_dark_regions.length==0){  
                    non_dark_regions.push([accession_id, null, null]);
            } 
            queryPromises.push(connector.queryPromise(non_dark_sql, [non_dark_regions]));
        }     
        return Promise.all(queryPromises);  
    };
var  get_dark_regions_from_accession = function(uniprot_primary_accession,finalCallback){
    var accession = {"Primary_Accession": uniprot_primary_accession};
    get_dark_regions(accession).then(function(results){
        finalCallback(results.accession,results.regions);
    });
}
var get_dark_regions = function(accession){
    return get_regions(accession,true)
}

var get_non_dark_regions = function(accession){
    return get_regions(accession,false)
}

var get_regions = function(accession,calculate_dark){
    var uniprot_primary_accession = accession.Primary_Accession;
    var accession_id = accession.Id;
    var time_id = Math.random()+'get_regions';
      console.time(time_id);	
      matches = {};
      matches.cachedHit = false;
      matches.start_date = new Date().getTime();
      var cacheKey = uniprot_primary_accession;
      // find all the protein sequences
      var result =  get_protein_sequence_sql(uniprot_primary_accession)
            .then(
          function(sqlQuery) {
            console.log('about to sequence');
            return connector.queryPromise(sqlQuery.sql, sqlQuery.args);
          }).then(function(results) {
            console.log('got sequence');
            // get the uniprot sequence data
            return sequence.get_uniprot_sequence(results, matches);
          }).then(function(sequences) {
            console.log('got sequences' + sequences);
            matches.sequences = sequences;
            if(!matches.sequences){
                console.log(sequences);
            }
            console.log('about to run generate regions ' + sequences);
            if (calculate_dark){
                return generate_dark_regions(uniprot_primary_accession, matches.sequences[0]);
            }else{
                return generate_non_dark_regions(uniprot_primary_accession, matches.sequences[0]);
            }
            
          }).then(function(regions) {
            console.log('got regions' + regions);
            matches.regions = regions;
            return { "accession":accession, "regions":regions};
          }).catch( function(err) {
            console.timeEnd(time_id);
            console.log(err);
            logger.info('error getting protein sequence sql (dark regions) : ' + err);
            reject(err);
            // throw err;
          });
          return result;
}

var get_protein_sequence_sql = function(primaryAccession) {
  return new Promise(
      function(resolve) {
        var sqlquery = "SELECT Primary_Accession, MD5_Hash as uniprot_hash, Sequence, Description, Length \
            FROM protein_sequence WHERE Primary_Accession = ?";
        var args = primaryAccession;
        //console.log('sql query: ' + sqlquery + ', args: ' + args);
        resolve({sql: sqlquery, args: args});
      });
};

function generate_non_dark_regions(uniprot_primary_accession, sequence) {
  'use strict';
  console.time('generate_non_dark_regions');

  var psshProcessor = new PSSHProcessor(sequence, null);

  return matching_structures_querier.getPSSHAndPDBRowsPromise(sequence, function (psshRow, chainRow) {
    psshProcessor.processPSSHRow_for_dark_proteome(psshRow, chainRow);
  }).then(
      function() {
        var sequence_length;
        if(sequence){
            sequence_length = sequence.length;
        }
        if(sequence_length<=0){
              console.log(sequence);
        }
        if (psshProcessor.regions.length > 0) {
          return pssh_regions_to_non_dark_regions(sequence_length,psshProcessor.regions);
        } else {
          return {"non_dark_regions":[]};
        }
      }).catch(function (err) {
        return {"error":err,"non_dark_regions":[]};
      });
}


function generate_dark_regions(uniprot_primary_accession, sequence) {
  'use strict';
  console.time('generate_dark_regions');

  var psshProcessor = new PSSHProcessor(sequence, null);

  return matching_structures_querier.getPSSHAndPDBRowsPromise(sequence, function (psshRow, chainRow) {
    psshProcessor.processPSSHRow_for_dark_proteome(psshRow, chainRow);
  }).then(
      function() {
        var sequence_length;
        if(sequence){
            sequence_length = sequence.length;
        }
        if(sequence_length<=0){
              console.log(sequence);
        }
        if (psshProcessor.regions.length > 0) {
          return pssh_regions_to_dark_regions(sequence_length,psshProcessor.regions);
        } else {
          return {"non_dark_regions":[],"dark_regions":[[1,sequence_length]]};
        }
      }).catch(function (err) {
        return {"error":err,"non_dark_regions":[],"dark_regions":[]};
      });
}

var addUniprot_regionsToPsshProcessor = function(uniprot_primary_accession,psshProcessor){
    return getUniprot_PDB(uniprot_primary_accession).then(
          function(sqlQuery) {
            return connector.queryPromise(sqlQuery.sql, sqlQuery.args);
          }).then(function(data){
        data.forEach(function(row){
            psshProcessor.regions.push([row.Start_residue,row.End_Residue]);
        })
    });
}

var getUniprot_PDB = function(uniprot_primary_accession){
  return new Promise(
      function(resolve) {
        var sqlquery = "SELECT Primary_Accession, PDB_Id, Start_Residue, End_Residue \
            FROM Uniprot_PDB WHERE Primary_Accession = ?";
        var args = uniprot_primary_accession;
        //console.log('sql query: ' + sqlquery + ', args: ' + args);
        resolve({sql: sqlquery, args: args});
      });
}

var pssh_regions_to_dark_regions = function(sequence_length,pssh_members){
    var results = {};
    if(!pssh_members){
        return results;
    }
    return get_region_results(sequence_length,pssh_members,true);
}

var pssh_regions_to_non_dark_regions = function(sequence_length,pssh_members){
    var results = {};
    if(!pssh_members){
        return results;
    }
    return get_region_results(sequence_length,pssh_members);
}

var get_region_results = function(sequence_length,regions,calculate_dark){
    var results = {};
    var non_dark_regions = find_non_dark_regions(regions);
    results.non_dark_regions = non_dark_regions;
    if(!calculate_dark||!sequence_length||sequence_length<1){
        return results;
    }    
    results.dark_regions = find_dark_regions(sequence_length,non_dark_regions);
    return results;
}

var find_dark_regions = function(sequence_length,non_dark_regions){
    var dark_regions = [];
    var n_non_dark_regions = non_dark_regions.length;
    var previous_start = 0;
    if (non_dark_regions&&non_dark_regions[0]&&non_dark_regions[0][0]>1){
        previous_start=1;
    }
    for(var non_dark_regions_index=0;non_dark_regions_index<n_non_dark_regions;non_dark_regions_index++) {
        var non_dark_region = non_dark_regions[non_dark_regions_index];
        if(previous_start>0){
            dark_regions.push([previous_start,non_dark_region[0]-1]);
        }
        previous_start = non_dark_region[1]+1;        
    }
    if(previous_start<sequence_length){
        dark_regions.push([previous_start,sequence_length]);
    }
    //console.log(dark_regions);
    return dark_regions
}

var find_non_dark_regions = function(regions){
    var non_dark_regions = [];
    var sorted_regions = regions.sort(compare_region);
    var n_regions = sorted_regions.length;
    var current_non_dark_region = region_from(sorted_regions[0]);
    for(var region_index=0;region_index<n_regions;region_index++) {
        var region = sorted_regions[region_index];
        if(current_non_dark_region.is_overlapped_by(region)){
            if(current_non_dark_region.is_extended_by(region)){
                current_non_dark_region.extend_to(region);
            }  
        } else{
            non_dark_regions.push(current_non_dark_region.value());
            current_non_dark_region = region_from(sorted_regions[region_index]);
        }        
    }
    non_dark_regions.push(current_non_dark_region.value());
    //console.log(non_dark_regions);
    return non_dark_regions;
}

var region_from = function(region){ 

    var current_non_dark_region =region;

    function value(){
        return current_non_dark_region;
    }

    function extend_to(region){
        current_non_dark_region[1] = region[1];
    }

    function is_overlapped_by(region){
        var region_start = region[0];
        var current_non_dark_region_start = current_non_dark_region[0];
        var current_non_dark_region_end = current_non_dark_region[1];
        return region_start==current_non_dark_region_start ||
            (region_start>current_non_dark_region_start && region_start<=current_non_dark_region_end+1)
        ;
    }
    function is_extended_by(region){
        var region_end = region[1];
        var current_non_dark_region_end = current_non_dark_region[1];
        return region_end>current_non_dark_region_end;
    }

    return {
        value:value,
        is_extended_by:is_extended_by,
        is_overlapped_by:is_overlapped_by,
        extend_to:extend_to
        }
}

function compare_region(a,b) {
    if (a[0] < b[0])
        return -1;
    if (a[0] > b[0])
        return 1;
    if (a[1] < b[1])
        return -1;
    if (a[1] > b[1])
        return 1;
    return 0;
}

exports.save_remaining_non_dark_regions = save_remaining_dark_regions;