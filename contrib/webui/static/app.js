// chronos console client helpers: auto-refresh polling, theme toggle, and
// confirmation for destructive bulk forms. No frameworks.
(function () {
  "use strict";

  // ---- theme ----
  var root = document.documentElement;
  var saved = localStorage.getItem("chronos-theme");
  if (saved) root.setAttribute("data-theme", saved);
  var themeBtn = document.getElementById("theme-toggle");
  if (themeBtn) {
    themeBtn.addEventListener("click", function () {
      var order = ["auto", "dark", "light"];
      var cur = root.getAttribute("data-theme") || "auto";
      var next = order[(order.indexOf(cur) + 1) % order.length];
      root.setAttribute("data-theme", next);
      localStorage.setItem("chronos-theme", next);
      themeBtn.title = "theme: " + next;
    });
  }

  // ---- auto refresh (dashboard only) ----
  var grid = document.getElementById("qgrid");
  var toggle = document.getElementById("refresh-toggle");
  if (grid && toggle) {
    var on = localStorage.getItem("chronos-refresh") !== "off";
    var timer = null;

    function render(q) {
      var card = grid.querySelector('[data-queue="' + q.queue + '"]');
      if (!card) return; // 새 큐는 다음 페이지 로드에서
      var map = { pending: q.pending, active: q.active, scheduled: q.scheduled, retry: q.retry, archived: q.archived, completed: q.completed };
      Object.keys(map).forEach(function (k) {
        var el = card.querySelector('[data-stat="' + k + '"]');
        if (el) el.textContent = map[k]; // data-stat elements hold only the number
      });
      var spark = card.querySelector("[data-spark]");
      if (spark && q.spark) spark.innerHTML = q.spark; // server-generated SVG only
      card.classList.toggle("warn", q.archived > 0);
    }

    function tick() {
      fetch("/api/stats").then(function (r) { return r.json(); }).then(function (data) {
        (data.queues || []).forEach(render);
      }).catch(function () { /* transient; keep polling */ });
    }

    function apply() {
      toggle.textContent = on ? "●" : "○";
      if (timer) { clearInterval(timer); timer = null; }
      if (on) { timer = setInterval(tick, 5000); tick(); }
    }
    toggle.addEventListener("click", function () {
      on = !on;
      localStorage.setItem("chronos-refresh", on ? "on" : "off");
      apply();
    });
    apply();
  }

  // ---- confirm destructive bulk forms ----
  document.querySelectorAll("form[data-confirm]").forEach(function (f) {
    f.addEventListener("submit", function (e) {
      if (!window.confirm(f.getAttribute("data-confirm"))) e.preventDefault();
    });
  });
})();
