
// See a Basic Example of bulk insert and update:
//http://stackoverflow.com/questions/31131127/bulk-create-in-meteor/32848790#32848790
//
// var bulk = Apple.rawCollection().initializeUnorderedBulkOp();
//
// bulk.insert( { _id: 1, item: "abc123", status: "A", soldQty: 5000 } );
// bulk.insert( { _id: 2, item: "abc456", status: "A", soldQty: 150 } );
// bulk.insert( { _id: 3, item: "abc789", status: "P", soldQty: 0 } );
//
// Meteor.wrapAsync(bulk.execute)();
// bulk.find({_id:1 }).update( { $set: { item: "Hello"} } );
// Meteor.wrapAsync(bulk.execute)();
// console.log( Apple.find({ _id: 1}).fetch() );

//start with clean database
Apple.remove({});
Aggregate.remove({});

// 1. ENRICH THE DATA
//Set-up the bulk operations

//This is for an ORDERED bulkOp. The order of inserts, and updates matter. (The inserts need to happen before the update)
var bulkSales = Apple.rawCollection().initializeOrderedBulkOp();
_.each(appleData, function(e){
  //use moment.js to parse dates from strings
  parsedDate = moment(e["Download Date (PST)"], "MM-DD-YY")
  //in moment.js january is month 0, needs offset
  e.month = parsedDate.month() + 1
  e.year = parsedDate.year()
  bulkSales.insert(e);
})

//for every region, create a bulk instruction to update the Apple records
_.each(regionData, function(e){
  bulkSales.find({"Country Code":e.CountryCode }).update( { $set: { region: e.Region} } );
})

//for each currency, match the date and insert that record.
_.each(currency, function(e){
  bulkSales.find({"Customer Currency":e["Customer Currency"], month:e.m, year: e.y}).update( { $set: { currencyValue: e.CurrencyValue} } );
})

//run all the bulk operations, insert and updates
// meteor needs to run this function in a fiber since it is asynchronous. Meteor has a helper function for cases like this: wrapAsync
Meteor.wrapAsync(bulkSales.execute)();


//2. AGGREGATE
var temp = Apple.rawCollection()
var aggregateQuery = Meteor.wrapAsync(temp.aggregate, temp);

var tempAgg = aggregateQuery([{
  $group: {
    _id: {//_id is a fixed property for mongo aggregate, cannot be changed
      region: "$region",
      title: "$Title",
      country: "$Country Code"
    },
    //project : "$project",
    dollarSales: {
      $sum: {
        $multiply: ["$Customer Price", "$currencyValue"]
      }
    }
  }
}]);

console.log(tempAgg);

//Change the data structure before persisting to mongo. Also using Bulk operation
var bulkAgg = Aggregate.rawCollection().initializeOrderedBulkOp();
_.each(tempAgg, function(e){
  e.region = e._id.region
  e.title = e._id.title
  e.country = e._id.country
  delete e._id
  bulkAgg.insert(e);
});
Meteor.wrapAsync(bulkAgg.execute)();
