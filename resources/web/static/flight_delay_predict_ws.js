var socket = io();
var pendingId = null;
var receivedPredictions = {};

// WebSocket: recibe predicciones de Spark via Flask
socket.on('prediction', function(data) {
  receivedPredictions[data.UUID] = data;
  if (pendingId && data.UUID === pendingId) {
    renderPage(data);
    pendingId = null;
  }
});

// Envío del formulario
$("#flight_delay_classification").submit(function(event) {
  event.preventDefault();

  var $form = $(this);
  var url   = $form.attr("action");

  var posting = $.post(url, $form.serialize());

  posting.done(function(data) {
    var response = JSON.parse(data);
    if (response.status === "OK") {
      pendingId = response.id;
      $("#result").empty().append("Processing...");

      // Si la predicción ya llegó antes de que se estableciera pendingId
      if (receivedPredictions[pendingId]) {
        renderPage(receivedPredictions[pendingId]);
        pendingId = null;
      }
    }
  });
});

// Renderiza la predicción en pantalla
// Buckets: [-inf, -15, 0, 30, +inf]
function renderPage(data) {
  var pred = data.Prediction !== undefined ? data.Prediction : data.prediction;
  var msg;

  if (pred == 0)      { msg = "Early (15+ Minutes Early)"; }
  else if (pred == 1) { msg = "Slightly Early (0-15 Minutes Early)"; }
  else if (pred == 2) { msg = "Slightly Late (0-30 Minute Delay)"; }
  else if (pred == 3) { msg = "Very Late (30+ Minutes Late)"; }
  else                { msg = "Prediction: " + pred; }

  $("#result").empty().append(msg);
}
