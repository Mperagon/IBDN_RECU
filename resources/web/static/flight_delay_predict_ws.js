var socket = io();
var pendingId = null;
var receivedFlink = {};
var receivedSpark  = {};

function bucketLabel(pred) {
  if (pred == 0)      return "Early (15+ min early)";
  if (pred == 1)      return "Slightly Early (0-15 min early)";
  if (pred == 2)      return "Slightly Late (0-30 min delay)";
  if (pred == 3)      return "Very Late (30+ min delay)";
  return "Prediction: " + pred;
}

socket.on('prediction_flink', function(data) {
  receivedFlink[data.UUID] = data;
  if (pendingId && data.UUID === pendingId) {
    var pred = data.Prediction !== undefined ? data.Prediction : data.prediction;
    $("#result_flink").text(bucketLabel(pred));
  }
});

socket.on('prediction_spark', function(data) {
  receivedSpark[data.UUID] = data;
  if (pendingId && data.UUID === pendingId) {
    var pred = data.Prediction !== undefined ? data.Prediction : data.prediction;
    $("#result_spark").text(bucketLabel(pred));
  }
});

$("#flight_delay_classification").submit(function(event) {
  event.preventDefault();
  var $form = $(this);

  $("#result_flink").text("Processing...");
  $("#result_spark").text("Processing...");

  $.post($form.attr("action"), $form.serialize()).done(function(data) {
    var response = JSON.parse(data);
    if (response.status === "OK") {
      pendingId = response.id;

      if (receivedFlink[pendingId]) {
        var pred = receivedFlink[pendingId].Prediction;
        $("#result_flink").text(bucketLabel(pred));
      }
      if (receivedSpark[pendingId]) {
        var pred = receivedSpark[pendingId].Prediction;
        $("#result_spark").text(bucketLabel(pred));
      }
    }
  });
});
