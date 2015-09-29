Template.hello.helpers({
  total: function (param) {

    test = Aggregate.find().fetch()

    var map = _.map(test, function(e){
      console.log(e);
      return e.dollarSales
    });

    var reduce = _.reduce(map , function(memo, num){ return memo + num; }, 0);

    return reduce;
  }
});
