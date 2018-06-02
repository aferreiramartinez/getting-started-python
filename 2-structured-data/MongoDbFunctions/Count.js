db.getCollection('eikonTwo').aggregate([
    {
        $match:{'PriceMainIndexRIC':'.IXIC'}
    },
    {
        $count:"sp500" //store result in variable sp500 and display it
    }
])