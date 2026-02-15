(function () {
  function detectLanguage(container) {
    var raw = (container.getAttribute("data-language") || "").trim().toLowerCase();
    if (!raw) {
      return "plaintext";
    }
    if (raw === "toml") {
      return "toml";
    }
    if (raw === "bash") {
      return "bash";
    }
    if (raw === "shell") {
      return "bash";
    }
    if (raw === "zsh") {
      return "bash";
    }
    return raw;
  }

  function applySyntaxHighlight(container) {
    if (!window.hljs) {
      return;
    }

    var lang = detectLanguage(container);
    var hasLang = !!window.hljs.getLanguage && window.hljs.getLanguage(lang);
    var rows = Array.from(container.querySelectorAll(".line-content"));

    rows.forEach(function (node) {
      var raw = (node.textContent || "").replace(/\u00a0/g, "");
      if (!raw.trim()) {
        node.innerHTML = "&nbsp;";
        return;
      }

      try {
        var value;
        if (hasLang) {
          value = window.hljs.highlight(raw, { language: lang, ignoreIllegals: true }).value;
        } else {
          value = window.hljs.highlightAuto(raw).value;
        }
        node.innerHTML = value || htmlEscape(raw);
      } catch (_err) {
        node.innerHTML = htmlEscape(raw);
      }
    });
  }

  function htmlEscape(s) {
    return s
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function parseRangeToken(token) {
    var match = (token || "").trim().match(/^L?(\d+)(?:-L?(\d+))?$/i);
    if (!match) {
      return null;
    }
    var start = Number(match[1]);
    var end = Number(match[2] || match[1]);
    if (!Number.isFinite(start) || !Number.isFinite(end) || start < 1 || end < 1) {
      return null;
    }
    if (end < start) {
      var tmp = start;
      start = end;
      end = tmp;
    }
    return { start: start, end: end };
  }

  function parseLinesSpec(spec) {
    if (!spec) {
      return [];
    }

    var selected = new Set();
    spec.split(",").forEach(function (chunk) {
      var part = chunk.trim();
      if (!part) {
        return;
      }

      var parsed = parseRangeToken(part);
      if (!parsed) {
        return;
      }

      for (var line = parsed.start; line <= parsed.end; line += 1) {
        selected.add(line);
      }
    });

    return Array.from(selected).sort(function (a, b) {
      return a - b;
    });
  }

  function linesFromLocation() {
    var params = new URLSearchParams(window.location.search);
    if (params.has("lines")) {
      return parseLinesSpec(params.get("lines"));
    }
    if (params.has("line")) {
      return parseLinesSpec(params.get("line"));
    }

    var hash = window.location.hash || "";
    var rangeMatch = hash.match(/^#(L\d+(?:-L?\d+)?)$/i);
    if (rangeMatch) {
      return parseLinesSpec(rangeMatch[1]);
    }

    return [];
  }

  function clearSelection(rows) {
    rows.forEach(function (row) {
      row.classList.remove("is-selected");
      row.classList.remove("is-focus");
      row.classList.remove("is-range-start");
      row.classList.remove("is-range-end");
    });
  }

  function centerOnRow(row) {
    var rect = row.getBoundingClientRect();
    var top = window.scrollY + rect.top - window.innerHeight / 2 + rect.height / 2;
    var clamped = Math.max(0, top);
    if (Math.abs(window.scrollY - clamped) < 2) {
      return;
    }
    window.scrollTo({ top: clamped, behavior: "smooth" });
  }

  function applySelection() {
    var container = document.querySelector(".source-code");
    if (!container) {
      return;
    }

    document.body.classList.add("source-viewer-page");
    applySyntaxHighlight(container);

    var rows = Array.from(container.querySelectorAll(".code-line[data-line]"));
    if (!rows.length) {
      return;
    }

    clearSelection(rows);

    var selected = linesFromLocation();
    if (!selected.length) {
      return;
    }

    var selectedSet = new Set(selected);
    var focusLine = selected[Math.floor(selected.length / 2)];
    var focusRow = null;

    rows.forEach(function (row) {
      var line = Number(row.getAttribute("data-line"));
      if (!selectedSet.has(line)) {
        return;
      }
      row.classList.add("is-selected");
      if (!selectedSet.has(line - 1)) {
        row.classList.add("is-range-start");
      }
      if (!selectedSet.has(line + 1)) {
        row.classList.add("is-range-end");
      }
      if (line === focusLine) {
        focusRow = row;
      }
    });

    if (!focusRow) {
      focusRow = document.getElementById("L" + selected[0]);
    }
    if (!focusRow) {
      return;
    }

    if (selected.length === 1) {
      focusRow.classList.add("is-focus");
    }
    centerOnRow(focusRow);
  }

  document.addEventListener("DOMContentLoaded", applySelection);
  window.addEventListener("hashchange", applySelection);
  window.addEventListener("popstate", applySelection);
})();
