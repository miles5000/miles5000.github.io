(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();
    var num_pages = 10;

    var api_key = "7adcc4ec446ae9574c6aa9e0178fa26f",
        base_uri = "https://api.themoviedb.org/3/",
        images_uri =  "https://image.tmdb.org/t/p/w500";

    // Define the schema
    myConnector.getSchema = function(schemaCallback) {
        var cols = [
            { id: "poster_path", dataType: tableau.dataTypeEnum.string },
            { id: "popularity", dataType: tableau.dataTypeEnum.float },
            { id: "id", dataType: tableau.dataTypeEnum.int },
        ];

        var tableSchema = {
            id: "movies",
            alias: "Top Rated Movies",
            columns: cols
        };

        schemaCallback([tableSchema]);
    };

    // Download the data
    myConnector.getData = function(table, doneCallback) {
        var i;
        var promises = [];

        for (i = 1; i < num_pages; i++) {
            promises.push(getResultsPromise(table, i));    
        }

        var promise = Promise.all(promises);

        promise.then(function(response) {
            doneCallback();
        }, function(error) {
            tableau.abortWithError(error);
        });
    };

    function getResultsPromise(table, pageNum) {
        return new Promise(function(resolve, reject) {
            var connectionUrl = base_uri + "/movie/popular?api_key=" + api_key + "&page=" + pageNum;
            
            var xhr = $.ajax({
                url: connectionUrl,
                dataType: 'json',
                success: function(data) {
                    var toRet = [];
                    
                    if (data.results) {
                        _.each(data.results, function(record) {               
                            entry = {
                                "poster_path": images_uri + record.poster_path,
                                "popularity": record.popularity,
                                "id": record.id,
                            };

                            toRet.push(entry)
                        });

                        table.appendRows(toRet);
                        resolve();
                    } else {
                        Promise.reject("No results found for ticker symbol: " + ticker);
                    }
                 },
                 error: function(xhr, ajaxOptions, thrownError) {
                     Promise.reject("error connecting to the yahoo stock data source: " + thrownError);
                 }
            });
        });
    };

    tableau.registerConnector(myConnector);

    $(document).ready(function() {
        $("#submitButton").click(function() {
            tableau.connectionName = "TVDB Data";
            tableau.submit();
        });
    });
})();
