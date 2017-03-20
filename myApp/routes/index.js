let d3 = require('d3');
let express = require('express');
let xlsxj = require("xlsx-to-json");
let router = express.Router();
var alasql = require('alasql');
var jsonQuery = require('json-query')
var fs = require('fs'),
  JSONStream = require('JSONStream'),
  es = require('event-stream');
const db = require('monk')('localhost/testDB');
const wells = db.get('wells');
const Pdp = db.get('pdp');
const TC1 = db.get('typeCurve1');
const TC2 = db.get('typeCurve2');
const TC3 = db.get('typeCurve3');

// let wells = [];
router.get('/wells', function(req, res, next) {
  wells.count({}).then(response => console.log(response));
  return wells.find({})
    .then(response => res.json(response))
    .catch((err) => next(err));
});

router.get('/wells/:id', function(req, res, next) {
  return wells.find({_id: req.params.id})
    .then(response => res.json(response))
    .catch((err) => next(err));
});

router.post('/wells', (req, res, next) => {
  let data = req.body;
  return wells.insert(data)
    .then(result => {
      res.json(result);
    }).catch((err) => next(err));
});

router.patch('/wells/:id', (req, res, next) => {
  return wells.update({_id: req.params.id}, {$set: req.body})
    .then(result => {
      res.json(result);
    }).catch((err) => next(err));
});

router.delete('/wells/:id', (req, res, next) => {
  return wells.findOneAndDelete({_id: req.params.id})
    .then((response) => {
      res.json({
        response,
        message: '🗑'
      });
    }).catch((err) => next(err));
});

router.get('/pdpwells/:id', function(req, res, next) {
  return Pdp.distinct("LEASE", {"Water_System" : req.params.id})
    .then(result => {
      res.json(result);
    });
});


router.get('/pdp/:id', function(req, res, next) {
  return Pdp.aggregate([
    {
      '$match' : {"Water_System" : req.params.id},
    },
    {
      '$group' : {
        '_id' : {
          'Date' : "$OUTDATE"
        },
        'total' : {'$sum': "$Gross_Water_Bbls"},
        'count' : {'$sum': 1}
      }
    }
  ]).then(result => res.json(result));
});
router.get('/TC1-5000', function(req, res, next) {
  TC1.count({}).then(response => console.log(response));
  return TC1.find({})
    .then(response => res.json(response))
    .catch((err) => next(err));
});
router.get('/TC1-7500', function(req, res, next) {
  TC2.count({}).then(response => console.log(response));
  return TC2.find({})
    .then(response => res.json(response))
    .catch((err) => next(err));
});
router.get('/TC1-1000', function(req, res, next) {
  TC3.count({}).then(response => console.log(response));
  return TC3.find({})
    .then(response => res.json(response))
    .catch((err) => next(err));
});

// router.get('/pdp/:id', (req, res, next) => {
//   return Pdp.find({"Water_System": 'W' + req.params.id}).then(result => res.json(result))
//     .catch((err) => next(err));
// });

router.get('/pdp', (req, res, next) => {
  return Pdp.find({}).then(result => res.json(result))
    .catch((err) => next(err));
});

router.get('/day/:id', function(req, res, next) {
  TC1.find({"Days" : +req.params.id}).then((result) => {
    let all = [result];
    TC2.find({"Days" : +req.params.id}).then((result2) => {
      all.push(result2);
      TC3.find({"Days" : +req.params.id}).then((result3) => {
        all.push(result3);
        res.json(all);
      });
    });
  }).catch((err) => next(err));
});

router.get('/water/:id', function(req, res, next) {
  return wells.find({"WATER_SYSTEM" : req.params.id})
    .then(response => res.json(response))
    .catch((err) => next(err));
});

router.get('/rig/:id', function(req, res, next) {
  return wells.find({"RIG" : `Rig ${req.params.id}`})
    .then(response => res.json(response))
    .catch((err) => next(err));
});

router.patch('/tc1/:id', (req, res, next) => {
  return TC1.update({_id: req.params.id}, {$set: req.body})
    .then(result => {
      res.json(result);
    }).catch((err) => next(err));
});

router.patch('/tc2/:id', (req, res, next) => {
  return TC2.update({_id: req.params.id}, {$set: req.body})
    .then(result => {
      res.json(result);
    }).catch((err) => next(err));
});

router.patch('/tc3/:id', (req, res, next) => {
  return TC3.update({_id: req.params.id}, {$set: req.body})
    .then(result => {
      res.json(result);
    }).catch((err) => next(err));
});


router.patch('/pdp/:id', (req, res, next) => {
  return Pdp.update({_id: req.params.id}, {$set: req.body})
    .then(result => {
      res.json(result);
    }).catch((err) => next(err));
});


// router.post('/', function(req, res, next) {
//   let data = req.body;
//   data = Object.keys(data);
//   data = JSON.parse(data);
//   // data.forEach(p => {
//   //   Pdp.insert(p);
//   // });
//   let holder = [];
//   holder.push(data);
//   holder = holder.reduce((a,b) => {
//     return a.concat(b);
//   });
//
//   holder.forEach(day => {
//     TC3.insert(day);
//   });
// });


module.exports = router;
