// $.get({url: "/fields", async: false, success: function(data){
//     for (x in data){
//         $("#fields-wrapper").append(
//             "<input class='field' id='"+x+"' placeholder='"+data[x]["label"]+"'>"
//         )
//     }
// }});

$(document).ready(function () {
  var file;
  $("#filelist").on("click", ".list-group-item", function () {
    var listItems = $(".list-group-item");
    for (let i = 0; i < listItems.length; i++) {
      listItems[i].classList.remove("active");
    }
    $(this).addClass("active");
    file = $(this).attr("id");
  });

  $.get({
    url: "/data",
    async: false,
    success: function (dataFiles) {
      for (let i = 0; i < dataFiles.length; i++) {
        if (i == 0) {
          var active = " active";
        } else {
          var active = "";
        }
        $("#filelist").append(
          `<a class="list-group-item list-group-item-light p-3${active}" id="${dataFiles[i]}">${dataFiles[i]}</a>`,
        );
      }
      file = dataFiles[0];
    },
  });

  function runQuery() {
    const query = $("#query").val();
    console.log(file);
    $.ajax({
      url: "/search",
      data: { query: JSON.stringify(query), filename: JSON.stringify(file) },
      success: function (data) {
        $("#results").html(data);
        $("table").DataTable();
      },
    });
  }

  // press enter
  $("#query").keypress(function (e) {
    if (e.which == 13) {
      var inputVal = $(this).val();
      runQuery();
    }
  });

  // button click
  $("#search").click(function () {
    runQuery();
  });
});
