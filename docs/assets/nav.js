// Shared chrome for every page: theme (persisted + follows the system until the
// visitor overrides it), active-link marking, mobile nav, plus the display
// polish — scroll-progress, topbar depth, and motion-safe scroll reveal.
// No framework, no network.
(() => {
  const root = document.documentElement;
  const KEY = "rmb-theme";
  const mqLight = window.matchMedia("(prefers-color-scheme: light)");
  const mqReduce = window.matchMedia("(prefers-reduced-motion: reduce)");

  // Resolve the boot theme: an explicit saved choice wins; otherwise follow the
  // system preference. Applied before <body> paints, so there is no flash.
  const saved = localStorage.getItem(KEY);
  if (saved === "light" || saved === "dark") {
    root.setAttribute("data-theme", saved);
  } else if (mqLight.matches) {
    root.setAttribute("data-theme", "light");
  }

  // Arm scroll-reveal only when JS runs and motion is welcome, so the content is
  // always visible if either is missing. Set here (pre-paint) to avoid a flash.
  if (!mqReduce.matches) root.classList.add("reveal-ready");

  const setMeta = () => {
    let m = document.querySelector('meta[name="theme-color"]');
    if (!m) {
      m = document.createElement("meta");
      m.setAttribute("name", "theme-color");
      document.head.appendChild(m);
    }
    m.setAttribute(
      "content",
      root.getAttribute("data-theme") === "light" ? "#e9edf2" : "#0c1119"
    );
  };
  setMeta();

  // Keep following the system while the visitor has not made an explicit choice.
  mqLight.addEventListener("change", (e) => {
    if (localStorage.getItem(KEY)) return;
    root.setAttribute("data-theme", e.matches ? "light" : "dark");
    setMeta();
    paintToggle();
  });

  let paintToggle = () => {};

  document.addEventListener("DOMContentLoaded", () => {
    const btn = document.querySelector("[data-theme-toggle]");
    const other = () =>
      root.getAttribute("data-theme") === "light" ? "dark" : "light";
    paintToggle = () => {
      if (btn) btn.textContent = "◐ " + other();
    };
    paintToggle();
    if (btn) {
      btn.addEventListener("click", () => {
        const next = other();
        root.setAttribute("data-theme", next);
        localStorage.setItem(KEY, next);
        setMeta();
        paintToggle();
      });
    }

    const here = location.pathname.split("/").pop() || "index.html";
    document.querySelectorAll("nav.links a").forEach((a) => {
      const href = a.getAttribute("href");
      if (href === here || (here === "index.html" && href === "./")) {
        a.classList.add("active");
      }
    });

    const bar = document.querySelector(".topbar");
    const burger = document.querySelector("[data-burger]");
    if (burger && bar) {
      burger.addEventListener("click", () => bar.classList.toggle("open"));
    }

    // Scroll-progress bar + topbar depth on scroll.
    const prog = document.createElement("div");
    prog.className = "scroll-progress";
    document.body.appendChild(prog);
    const onScroll = () => {
      const max = root.scrollHeight - root.clientHeight;
      prog.style.transform = `scaleX(${max > 0 ? root.scrollTop / max : 0})`;
      if (bar) bar.classList.toggle("scrolled", root.scrollTop > 8);
    };
    addEventListener("scroll", onScroll, { passive: true });
    addEventListener("resize", onScroll, { passive: true });
    onScroll();

    // Motion-safe scroll reveal: cards fade up as they enter, staggered within
    // their row. Above-the-fold cards resolve immediately.
    if (!mqReduce.matches && "IntersectionObserver" in window) {
      const io = new IntersectionObserver(
        (entries) => {
          entries.forEach((e) => {
            if (e.isIntersecting) {
              e.target.classList.add("in");
              io.unobserve(e.target);
            }
          });
        },
        { rootMargin: "0px 0px -6% 0px", threshold: 0.06 }
      );
      document.querySelectorAll("section.band .card").forEach((card) => {
        const sibs = [...card.parentElement.children].filter((c) =>
          c.classList.contains("card")
        );
        card.style.transitionDelay = Math.max(0, sibs.indexOf(card)) * 70 + "ms";
        io.observe(card);
      });
    }
  });
})();
