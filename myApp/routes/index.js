let d3 = require('d3');
let express = require('express');

let router = express.Router();
var alasql = require('alasql');


const db = require('monk')('localhost/testDB');
const wells = db.get('wells');
const Pdp = db.get('pdp');
const TC1 = db.get('typeCurve1');
const TC2 = db.get('typeCurve2');
const TC3 = db.get('typeCurve3');

function isValidWell(well) {
  let validRig = typeof well.RIG == 'string' &&
                  well.RIG.trim().length > 1 &&
                  well.RIG.trim().length < 20;
  let validWell = typeof well.WELL == 'string' &&
                  well.WELL.trim().length > 1 &&
                  well.WELL.trim().length < 20;
  let validSystem = typeof well.WATER_SYSTEM == 'string' &&
                  well.WATER_SYSTEM.trim().length > 1 &&
                  well.WATER_SYSTEM.trim().length < 20;
  let validTC = typeof well.TYPE_CURVE == 'string' &&
                  well.TYPE_CURVE.trim().length > 1 &&
                  well.TYPE_CURVE.trim().length < 20;
  let validSpudSpud = Number(well.SPUD_SPUD) != NaN;
  let validSpud = (new Date(well.SPUD) !== "Invalid Date") && !isNaN(new Date(well.SPUD));
  return validRig && validWell && validSystem && validTC && validSpudSpud && validSpud;
}


router.get('/wells', function(req, res, next) {
  wells.count({}).then(response => console.log(response));
  return wells.find({})
    .then(response => res.json(response))
    .catch((err) => next(err));
});

router.post('/wells', function(req, res, next) {
  let well = req.body;
  if (isValidWell(well)) {
    let newWell = {
      RIG: well.RIG,
      WELL: well.WELL,
      "Why Scheduled?": null,
      WI: null,
      STATUS: null,
      AREA: null,
      WATER_SYSTEM: well.WATER_SYSTEM,
      TYPE_CURVE: well.TYPE_CURVE,
      TZ: null,
      "LAT LEN": null,
      SHL: null,
      BHL: null,
      PAD: null,
      "SPUD-SPUD": well.SPUD_SPUD,
      SPUD: well.SPUD,
      "RIG RELEASE": null,
      LAND: null,
      MSB: null,
      "Net Ac @ Risk": null,
      MPB: null,
      COMPLETION: null,
      NOTE: null,
      "GEO OPS": null,
      DRILLING: null,
      COMPL: null,
      "SURVEY REQUEST": null,
      RTS: null,
      STAKED: null,
      PLAT: null,
      GEOPROG: null,
      "DIR PLAN": null,
      AFE: null,
      NPZ: null,
      "SWR-37": null,
      W1: null,
      GAU: null,
      "SWR-13": null,
      WWP: null,
      "REGUL RTD": null,
      SUA: null,
      ACCESS: null,
      FACILITIES: null,
      "SURF RTB": null,
      "LOC BUILT": null,
      "MINERAL RTD": null,
      "FULL RTD": null
    };
    wells.insert(newWell);
    res.json({
      message: 'Recieved'
    });
  } else {
    next(new Error('Invalid Well Submission'));
  }
});

router.get('/allsystems', function(req, res, next) {
  return wells.distinct("WATER_SYSTEM")
    .then(response => res.json(response))
    .catch((err) => next(err));
});

router.get('/allrigs', function(req, res, next) {
  return wells.distinct("RIG")
    .then(response => res.json(response))
    .catch((err) => next(err));
});

router.get('/alltc', function(req, res, next) {
  return wells.distinct("TYPE_CURVE")
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
  return wells.find({"RIG" : req.params.id})
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


router.get('/waterSystemDaily/:id', (req, res, next) => {
  var pdp;
  var same = [];
  Pdp.aggregate([
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
  ])
  .then(result => {
    if (result.length > 0) {
      pdp = result.map(pdp => {
        let myArr = [];
        let month = pdp._id.Date.split('/')[0];
        let days = pdp._id.Date.split('/')[1];
        let year = pdp._id.Date.split('/')[2];
        let i = days;
        while (i > 0) {

          myArr.push({
            day: convertDate(month + '/' + i + '/' + year, 0),
            total: Math.round(pdp.total / days)
          });
          i--;
        }
        return myArr;
      });
      pdp = pdp.reduce((a, b) => {
        return a.concat(b);
      });
    } else {
      pdp = [];
    }
    return pdp;
  })
  .then(pdp => {
    return wells.find({"WATER_SYSTEM" : req.params.id})
      .then(system => {
        let totals = [];
        for (var i = 0; i < system.length; i++) {
          totals.push(findValues(system[i]));
        }
        return totals;
      });
  })
  .then(totals => {
    return Promise.all(totals).then(total => {
      let myArr = [];
      total.forEach(well => {
        for (var key in well) {
          let date = convertDate(key, 0);
          let dateObj = {
            day: date,
            total: well[key]
          };
          var found = myArr.some(function (el) {
            return el.day === dateObj.day;
          });
          if (!found) {
            myArr.push(dateObj);
          }
          else {
            for (var i = 0; i < myArr.length; i++) {
              if (myArr[i].day == dateObj.day) {
                myArr[i].total += dateObj.total;
              }
            }
          }
        }
      });
      return myArr;
    });
  })
  .then(myArr => {
    if (pdp.length > 0) {
      for (var j = 0; j < pdp.length; j++) {
        var found = same.some(function (el) {
          return el.day === pdp[j].day;
        });
        if (!found) { same.push({
          Day: pdp[j].day,
          PDP: numberWithCommas(pdp[j].total),
          New_Wells: 0,
          Total: pdp[j].total
        }); }
      }
      for (var i = 0; i < myArr.length; i++) {
        for (var j = 0; j < pdp.length; j++) {
          if (myArr[i].day == pdp[j].day) {
            same[j]['New_Wells'] = numberWithCommas(Number(same[j]['New_Wells']) + myArr[i].total);
            same[j]['Total'] = numberWithCommas(Number(same[j]['Total']) + myArr[i].total);
          }
        }
      }
      return same;
    } else {
      for (var i = 0; i < myArr.length; i++) {
        same.push({
          Day: myArr[i].day,
          PDP: 0,
          'New_Wells': numberWithCommas(myArr[i].total),
          Total: numberWithCommas(myArr[i].total)
        });
      }
    }
    return same;
  })
  .then(same => {
    res.json(same);
  });
});






router.get('/waterSystem/:id', (req, res, next) => {
  var pdp;
  var same = [];
  Pdp.aggregate([
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
  ])
  .then(result => {
    if (result.length > 0) {
      result.forEach(month => {
        month._id.Date = convertDate(month._id.Date, 0);
      });
      result.sort(function(c,d){
        var rx = /(\d+)\/(\d+)\/(\d+)/;
        var a = Number(c._id.Date.replace(rx, '$3$1$20000'));
        var b = Number(d._id.Date.replace(rx, '$3$1$20000'));
        return a < b ? -1 : a == b ? 0 : 1;
      });
      pdp = result.map(pdp => {
        return {
          month: pdp._id.Date,
          total: pdp.total
        };
      });
    }
    else {
      pdp = [];
    }
    return pdp;
  })
  .then(pdp => {
    return wells.find({"WATER_SYSTEM" : req.params.id})
      .then(response => {
        response.forEach(well => {
          if (well.SPUD) {
            well.SPUD = convertDate(well.SPUD, 0);
          }
          if (well.COMPLETION) {
            well.COMPLETION = convertDate(well.COMPLETION, 0);
          }
        });
        return response;
      });
  })
  .then(waterSystem => {
    let totals = [];
    for (var i = 0; i < waterSystem.length; i++) {
      totals.push(updateWater(waterSystem[i]));
    }
    return totals;
  })
  .then((totals) => {
    return Promise.all(totals).then(total => {
      let myArr = [];
      total.forEach(well => {
        for (var key in well) {
          let newKey = convertDate(key, 0);
          let newMonth = {
            month: newKey,
            total : well[key]
          };
          var found = myArr.some(function (el) {
            return el.month === newMonth.month;
          });
          if (!found) {
            myArr.push(newMonth);
          }
          else {
            for (var i = 0; i < myArr.length; i++) {
              if (myArr[i].month == newMonth.month) {
                myArr[i].total += newMonth.total;
              }
            }
          }
        }
      });
      return myArr;
    });
  })
  .then((myArr) => {
    if (pdp.length > 0) {
      for (var j = 0; j < pdp.length; j++) {
        var found = same.some(function (el) {
          return el.month === pdp[j].month;
        });
        if (!found) { same.push({
          Month: pdp[j].month,
          PDP: numberWithCommas(pdp[j].total),
          New_Wells: 0,
          Total: pdp[j].total
        }); }
      }
      for (var i = 0; i < myArr.length; i++) {
        for (var j = 0; j < pdp.length; j++) {
          if (myArr[i].month == pdp[j].month) {
            same[j]['New_Wells'] = numberWithCommas(Number(same[j]['New_Wells']) + myArr[i].total);
            same[j]['Total'] = numberWithCommas(Number(same[j]['Total']) + myArr[i].total);
          }
        }
      }
    } else {
      for (var i = 0; i < myArr.length; i++) {
        same.push({
          Month: myArr[i].month,
          PDP: 0,
          'New_Wells': numberWithCommas(myArr[i].total),
          Total: numberWithCommas(myArr[i].total)
        });
      }
    }
    return same;
  })
  .then(same => {
    res.json(same);
  });
});


function findValues(well) {
  if (!well.COMPLETION) {
    well.COMPLETION = convertDate(well.SPUD, 60);
  }
  let total = {};
  let monthDayCount = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  let month = Number(well.COMPLETION.split('/')[0]);
  let day = Number(well.COMPLETION.split('/')[1]);
  let year = Number(well.COMPLETION.split('/')[2]);
  if (well.TYPE_CURVE == 'TC1-5000') {
    return TC1.find({})
    .then(tc => {
      let i = 0;
      while (i < tc.length) {
        total[`${month}/${day}/${year}`] = Math.round(tc[i].Water);
        i++;
        if (day < monthDayCount[month]) {
          day++;
        } else {
          day = 1;
          if (month < 12) {
            month++;
          } else {
            month = 1;
            year++;
          }
        }
      }
      return total;
    });
  } else if (well.TYPE_CURVE == 'TC1-7500') {
    return TC2.find({})
    .then(tc => {
      let i = 0;
      while (i < tc.length) {
        total[`${month}/${day}/${year}`] = Math.round(tc[i].Water);
        i++;
        if (day < monthDayCount[month]) {
          day++;
        } else {
          day = 1;
          if (month < 12) {
            month++;
          } else {
            month = 1;
            year++;
          }
        }
      }
      return total;
    });
  } else if (well.TYPE_CURVE == 'TC1-1000') {
    return TC3.find({})
    .then(tc => {
      let i = 0;
      while (i < tc.length) {
        total[`${month}/${day}/${year}`] = Math.round(tc[i].Water);
        i++;
        if (day < monthDayCount[month]) {
          day++;
        } else {
          day = 1;
          if (month < 12) {
            month++;
          } else {
            month = 1;
            year++;
          }
        }
      }
      return total;
    });
  }
}





function updateWater(well) {
  let totals = {};
  let monthDayCount = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (!well.COMPLETION) {
    well.COMPLETION = convertDate(well.SPUD, 60);
  }
  let month = Number(well.COMPLETION.split('/')[0]);
  let day = Number(well.COMPLETION.split('/')[1]);
  let year = Number(well.COMPLETION.split('/')[2]);
  let dayCount = (monthDayCount[month] - day) + 1;
  if (well.TYPE_CURVE == 'TC1-5000') {
    return TC1.find({})
    .then(response => {
      let tc = response;
      let first = tc.slice(0, dayCount);
      let total = first.reduce((a,b) => {
        return a + b.Water;
      },0);
      if (!totals[month + '/' + monthDayCount[month] + '/' + year]) {
        totals[month + '/' + monthDayCount[month] + '/' + year] = Math.round(total);
      }
      else {
        totals[month + '/' + monthDayCount[month] + '/' + year] += Math.round(total);
      }
      if (month < 12) {
        month++;
      } else {
        month = 1;
        year++;
      }
      while (dayCount < tc.length) {
        let prev = dayCount;
        dayCount += monthDayCount[month];
        let first = tc.slice(prev, dayCount);
        let total = first.reduce((a,b) => {
          return a + b.Water;
        },0);
        if (!totals[month + '/' + monthDayCount[month] + '/' + year]) {
          totals[month + '/' + monthDayCount[month] + '/' + year] = Math.round(total);
        }
        else {
          totals[month + '/' + monthDayCount[month] + '/' + year] += Math.round(total);
        }
        if (month < 12) {
          month++;
        } else {
          month = 1;
          year++;
        }
      }
      return totals;
    });
  } else if (well.TYPE_CURVE == 'TC1-7500') {
    return TC2.find({})
    .then(response => {
      let tc = response;
      let first = tc.slice(0, dayCount);
      let total = first.reduce((a,b) => {
        return a + b.Water;
      },0);
      if (!totals[month + '/' + monthDayCount[month] + '/' + year]) {
        totals[month + '/' + monthDayCount[month] + '/' + year] = Math.round(total);
      }
      else {
        totals[month + '/' + monthDayCount[month] + '/' + year] += Math.round(total);
      }
      if (month < 12) {
        month++;
      } else {
        month = 1;
        year++;
      }
      while (dayCount < tc.length) {
        let prev = dayCount;
        dayCount += monthDayCount[month];
        let first = tc.slice(prev, dayCount);
        let total = first.reduce((a,b) => {
          return a + b.Water;
        },0);
        if (!totals[month + '/' + monthDayCount[month] + '/' + year]) {
          totals[month + '/' + monthDayCount[month] + '/' + year] = Math.round(total);
        }
        else {
          totals[month + '/' + monthDayCount[month] + '/' + year] += Math.round(total);
        }
        if (month < 12) {
          month++;
        } else {
          month = 1;
          year++;
        }
      }
      return totals;
    });
  } else if (well.TYPE_CURVE == 'TC1-1000') {
    return TC3.find({})
    .then(response => {
      let tc = response;
      let first = tc.slice(0, dayCount);
      let total = first.reduce((a,b) => {
        return a + b.Water;
      },0);
      if (!totals[month + '/' + monthDayCount[month] + '/' + year]) {
        totals[month + '/' + monthDayCount[month] + '/' + year] = Math.round(total);
      }
      else {
        totals[month + '/' + monthDayCount[month] + '/' + year] += Math.round(total);
      }
      if (month < 12) {
        month++;
      } else {
        month = 1;
        year++;
      }
      while (dayCount < tc.length) {
        let prev = dayCount;
        dayCount += monthDayCount[month];
        let first = tc.slice(prev, dayCount);
        let total = first.reduce((a,b) => {
          return a + b.Water;
        },0);
        if (!totals[month + '/' + monthDayCount[month] + '/' + year]) {
          totals[month + '/' + monthDayCount[month] + '/' + year] = Math.round(total);
        }
        else {
          totals[month + '/' + monthDayCount[month] + '/' + year] += Math.round(total);
        }
        if (month < 12) {
          month++;
        } else {
          month = 1;
          year++;
        }
      }
      return totals;
    });
  }
}




function convertDate(strDate, days) {
  let someDate = new Date(strDate);
  someDate.setDate(someDate.getDate() + days);
  return strDate = ('0' +(someDate.getMonth()+1)).slice(-2) + "/" + ('0' + someDate.getDate()).slice(-2) + "/" + someDate.getFullYear();
};

function numberWithCommas(x) {
  return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
};


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
