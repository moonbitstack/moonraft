// The page scroll bar keeps its place and stays out of the way at the top: the
// thumb is transparent while the page is at its start and visible from the first
// scroll on, so the first screen stays clean and nothing shifts when it appears.
const root = document.documentElement;
const mark = () => root.classList.toggle("at-top", scrollY === 0);
addEventListener("scroll", mark, { passive: true });
addEventListener("resize", mark, { passive: true });
mark();
