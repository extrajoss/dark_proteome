var check_progress = function (){            
            $.getJSON({
                url: "/save_non_dark_regions_progress",
                success: display_progress
            })
            .fail(function(err) {
                console.log('Error: ' ,err);
            })
            .always(function(err) {
                console.log("Loaded");
            });
        };  
var display_progress = function(res) {    
        console.log(res,res.progress); 
        var testTime = new Date(res.testTime);
        console.log(testTime);
        $("#progress").append("<tr><td>"+res.progress+"</td><td>"+ testTime.toISOString().slice(11,23)+"</td></tr>");
        setTimeout(check_progress,60000);
    };