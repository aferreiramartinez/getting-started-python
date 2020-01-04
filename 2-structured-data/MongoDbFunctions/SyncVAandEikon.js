db.visibleAlphaLatest.find().forEach(function(doc){
    var ticker = doc.EikonTicker
    print(ticker)
    db.eikonTwo.update(
	    { "EikonTicker": ticker },
	    { $set: { "VisibleAlpha.DataByFiscalQuarter": doc.DataByFiscalQuarter,  "VisibleAlpha.DataByFiscalYear": doc.DataByFiscalYear } }
	)
});