function loadText(textID) {
  $.get({
    url: "/textrecords",
    async: false,
    data: { textID: textID },
    success: function (data) {
      var exampleIDs = data;
      console.log(
        "Loaded text " + textID + " (" + exampleIDs.length + "records)",
      );
      $("#examples li").remove();
      for (let i = 0; i < exampleIDs.length; i++) {
        $.get({
          url: "/example",
          data: { id: exampleIDs[i] },
          async: false,
          success: function (data) {
            $("#examples").append(data);
          },
        });
        fitAll();
      }
    },
  });
}

$(".textLink").bind("click", function () {
  loadText($(this).attr("id"));
});

$(document).ready(function () {
  $("#dataExport").click(function () {
    $.ajax({
      url: "/export",
    });
  });
});

// var input = document.querySelector('input');
// console.log(input) // get the input element
// input.addEventListener('input', resizeInput); // bind the "resizeInput" callback on "input" event
// resizeInput.call(input); // immediately call the function

// $('input').on('input', (e) => {
//     console.log(e)
//     })

function fit(item) {
  if (item.value || !item.placeholder) {
    item.style.width = item.value.length + 1 + "ch";
  }
}

$(document).on("input", "input", function (event) {
  fit(this);
});

$(document).on("change", "input", function (event) {
  res = $(this).val();
  id = $(this).attr("id");
  exampleItem = $(this).parents("li")[0];
  $.ajax({
    url: "/update",
    data: { value: res, target: id },
  });
});

function fitAll() {
  inputs = $("input");
  var i = 0,
    len = inputs.length;
  while (i < len) {
    fit(inputs[i]);
    i++;
  }
}

$(function () {
  $(document).on("keypress", "input", function (event) {
    var keycode = event.keyCode ? event.keyCode : event.which;
    if (keycode == "13") {
      res = $(this).val();
      id = $(this).attr("id");
      exampleItem = $(this).parents("li")[0];
      $.ajax({
        url: "/update",
        data: { value: res, target: id },
        success: function (data) {
          $.ajax({
            url: "/example",
            data: { id: exampleItem.id },
            success: function (result) {
              result = $.parseHTML(result);
              exampleItem.replaceWith(result[1]);
              fitAll();
            },
          });
        },
      });
    }
  });
});

$("body")
  .off("click", ".dropdown-menu li a")
  .on("click", ".dropdown-menu li a", function (event) {
    example_item = $(this).parents("li.example")[0];
    value = $(this).text();
    $.ajax({
      url: "/pick",
      data: { target: $(this).attr("id"), choice: value },
      success: function (result) {
        result = $.parseHTML(result);
        example_item.replaceWith(result[1]);
        fitAll();
      },
    });
  });
