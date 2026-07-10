// Shared chrome for every page: theme toggle (persisted), active-link marking,
// and a mobile nav toggle. No framework, no network.
(() => {
  const root = document.documentElement;
  const KEY = "rmb-theme";
  const saved = localStorage.getItem(KEY);
  if (saved === "light" || saved === "dark") {
    root.setAttribute("data-theme", saved);
  } else if (window.matchMedia("(prefers-color-scheme: light)").matches) {
    root.setAttribute("data-theme", "light");
  }

  document.addEventListener("DOMContentLoaded", () => {
    const btn = document.querySelector("[data-theme-toggle]");
    const label = () =>
      (root.getAttribute("data-theme") === "light" ? "dark" : "light");
    const paint = () => {
      if (btn) btn.textContent = "◐ " + label();
    };
    paint();
    if (btn) {
      btn.addEventListener("click", () => {
        const next =
          root.getAttribute("data-theme") === "light" ? "dark" : "light";
        root.setAttribute("data-theme", next);
        localStorage.setItem(KEY, next);
        paint();
      });
    }

    const here = location.pathname.split("/").pop() || "index.html";
    document.querySelectorAll("nav.links a").forEach((a) => {
      const href = a.getAttribute("href");
      if (href === here || (here === "index.html" && href === "./")) {
        a.classList.add("active");
      }
    });

    const burger = document.querySelector("[data-burger]");
    const bar = document.querySelector(".topbar");
    if (burger && bar) {
      burger.addEventListener("click", () => bar.classList.toggle("open"));
    }
  });
})();
