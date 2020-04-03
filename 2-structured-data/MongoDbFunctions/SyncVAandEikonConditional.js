db.visibleAlphaLatest.find().forEach(function(doc){
    var ticker = doc.EikonTicker
    print(ticker)
    db.eikonTwo.update(
	    { "EikonTicker": ticker, "VisibleAlpha" : {$exists : false}},
	    { $set: { "VisibleAlpha.DataByFiscalQuarter": doc.DataByFiscalQuarter,  "VisibleAlpha.DataByFiscalYear": doc.DataByFiscalYear } }
	)
});