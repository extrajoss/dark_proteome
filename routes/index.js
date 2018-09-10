var express = require('express');
var router = express.Router();
var dark_proteome_calculations = require(
  '../server/dark_proteome_calculations.js');
var dark_proteome_postProcessing = require(
  '../server/dark_proteome_postProcessing')

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', {
    title: 'Express'
  });
});

var start_post_processing = function(request, response, next) {
  return dark_proteome_postProcessing.start_post_processing(request.query.suffix)
    .then(function(data) {
      next();
    });
}

var save_non_dark_regions = function(request, response, next) {
  return dark_proteome_calculations.save_remaining_non_dark_regions()
    .then(function(data) {
      next();
    });
}

var save_dark_regions = function(request, response, next) {
  return dark_proteome_calculations.save_remaining_dark_regions()
    .then(function(data) {
      next();
    });
}

var dark_regions_json = function(request, response, next, options, separator,
  mimetype) {
  'use strict';
  if (
    typeof request.params.id !== 'undefined' &&
    (
      request.params.id.match(
        /([O,P,Q][0-9][A-Z,0-9][A-Z,0-9][A-Z,0-9][0-9])||([A-N,R-Z][0-9][A-Z][A-Z,0-9][A-Z,0-9][0-9])||([A-N,R-Z][0-9][A-Z][A-Z,0-9][A-Z,0-9][0-9][A-Z][A-Z,0-9][A-Z,0-9][0-9])$/
      )
    )
  ) {
    dark_proteome_calculations.get_dark_regions(request, response, next,
        options)
      .then(function(data) {
        try {

          var regions = data.regions;
          var json = JSON.stringify(regions, null, "\t");
          write_response_string(response, json, mimetype);
        } catch (e) {
          console.log('e: ' + e);
          throw e;
        }
      });
  } else {
    next();
  }
}

var write_response_string = function(response, body, type) {
  response.writeHead(200, {
    'Content-Length': body.length,
    'Content-Type': 'text/plain'
  });
  response.end(body);
}

router.get('/start_post_processing', function(request, response, next) {
  var startTime = new Date().toISOString();
  start_post_processing(request, response, next);
  response.render('start_post_processing', {
    title: "start_post_processing",
    startTime: startTime
  });
});

router.get('/save_non_dark_regions', function(request, response, next) {
  var startTime = new Date().toISOString();
  save_non_dark_regions(request, response, next);
  response.render('save_non_dark_regions', {
    title: "save_non_dark_regions",
    startTime: startTime
  });
});

router.get('/save_dark_regions', function(request, response, next) {
  var startTime = new Date().toISOString();
  save_dark_regions(request, response, next);
  response.render('save_dark_regions', {
    title: "save_dark_regions",
    startTime: startTime
  });
});

router.get('/save_dark_regions', function(request, response, next) {
  var startTime = new Date().toISOString();
  save_dark_regions(request, response, next);
  response.render('save_dark_regions', {
    title: "save_dark_regions",
    startTime: startTime
  });
});

router.get('/save_non_dark_regions_progress', function(request, response, next) {
  dark_proteome_calculations.check_progress()
    .then(function(data) {
      var results = data[0];
      results.testTime = new Date();
      var body = JSON.stringify(results);
      response.write(body);
      response.end();
    })
});

router.get('/save_dark_regions_progress', function(request, response, next) {
  dark_proteome_calculations.check_progress()
    .then(function(data) {
      var results = data[0];
      results.testTime = new Date();
      var body = JSON.stringify(results);
      response.write(body);
      response.end();
    })
});

router.get('/dark_regions/:id', function(request, response, next) {
  dark_regions_json(request, response, next);
});

router.get('/dark_regions_old/:id', function(request, response, next) {
  dark_regions_json(request, response, next, {
    "database": "pssh2"
  });
});

module.exports = router;