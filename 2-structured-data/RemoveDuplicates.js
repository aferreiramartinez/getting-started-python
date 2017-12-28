db.fundamentals.aggregate([
  { $group: {
    _id: { Ticker: "$Ticker" },   // replace `name` here twice
    uniqueIds: { $addToSet: "$_id" },
    count: { $sum: 1 } 
  } }, 
  { $match: { 
    count: { $gt: 1 } 
  } },
  { $sort : { count : -1} },
  { $limit : 10 }
]);
  