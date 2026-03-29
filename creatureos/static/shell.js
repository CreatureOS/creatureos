(() => {
  const LS_SIDEBAR_COLLAPSED = "creatureos.sidebarCollapsed.v1";
  const LS_TOKEN = "creatureos.token.v1";
  const MOBILE_QUERY = "(max-width: 900px), (max-height: 520px) and (orientation: landscape)";
  const IS_ANDROID = /\bAndroid\b/i.test(String(navigator.userAgent || ""));

  const isMobileViewport = () => {
    try {
      return globalThis.matchMedia?.(MOBILE_QUERY)?.matches ?? (window.innerWidth <= 900);
    } catch {
      const w = window.innerWidth;
      const h = window.innerHeight;
      const isLandscape = w > h;
      return w <= 900 || (isLandscape && h <= 520);
    }
  };

  // Mobile browsers (especially older iOS/iPadOS) can report 100vh larger than the
  // actually visible viewport, which can push bottom UI (like the composer) under
  // the OS/browser chrome. Use VisualViewport (when available) to keep layout
  // constrained to the visible area.
  //
  // Also: some browsers aggressively "resize" the viewport when the on-screen keyboard
  // opens, which can make the whole page jump. We freeze the app height while a keyboard
  // is detected and instead expose a keyboard-height var for the composer to move.
  const KEYBOARD_OPEN_THRESHOLD_PX = 120;
  const KEYBOARD_TRANSITION_THRESHOLD_PX = 24;
  let lastNonKeyboardHeight = 0;
  let lastNonKeyboardWidth = 0;
  const isTextInputElement = (el) => {
    if (!el || el === document.body || el === document.documentElement) return false;
    const tag = String(el.tagName || "").toLowerCase();
    if (tag === "textarea") return true;
    if (tag === "input") {
      const type = String(el.getAttribute("type") || "text").toLowerCase();
      // Treat common text-entry inputs as keyboard-triggering.
      return ![
        "button",
        "checkbox",
        "color",
        "file",
        "hidden",
        "image",
        "radio",
        "range",
        "reset",
        "submit",
      ].includes(type);
    }
    return Boolean(el.isContentEditable);
  };
  const updateViewportVars = () => {
    const root = document.documentElement;
    if (!root) return;
    const vv = window.visualViewport;
    const layoutHeight = window.innerHeight;
    const layoutWidth = window.innerWidth;
    const clientHeight = root.clientHeight || layoutHeight;
    const vvHeight = vv?.height ?? clientHeight;
    const offsetTop = vv?.offsetTop ?? 0;
    const measuredHeight = Math.min(layoutHeight, clientHeight, vvHeight);

    // Only apply keyboard heuristics on mobile-ish viewports; on desktop resizing while
    // focused can look like a "keyboard" if we don't gate this.
    const focusIsTextInput = isMobileViewport() && isTextInputElement(document.activeElement);

    // Reset the "non-keyboard" baseline on major width changes (rotation / split screen).
    if (!lastNonKeyboardHeight || Math.abs(layoutWidth - lastNonKeyboardWidth) > 80) {
      lastNonKeyboardHeight = measuredHeight;
      lastNonKeyboardWidth = layoutWidth;
    }

    // IMPORTANT: Don't let the baseline "chase" the viewport down while the keyboard is
    // animating open (common on Android). If we do, the computed keyboard height stays
    // too small and the composer ends up behind the keyboard.
    if (!focusIsTextInput) {
      lastNonKeyboardHeight = measuredHeight;
      lastNonKeyboardWidth = layoutWidth;
    } else if (measuredHeight > lastNonKeyboardHeight) {
      // Capture any last-moment viewport expansion (URL bar hiding) before the keyboard opens.
      lastNonKeyboardHeight = measuredHeight;
      lastNonKeyboardWidth = layoutWidth;
    }

    const keyboardDelta = Math.max(0, lastNonKeyboardHeight - measuredHeight);
    const keyboardInTransition = focusIsTextInput && keyboardDelta >= KEYBOARD_TRANSITION_THRESHOLD_PX;
    const keyboardOpen = focusIsTextInput && keyboardDelta >= KEYBOARD_OPEN_THRESHOLD_PX;
    const keyboardHeight = keyboardInTransition ? keyboardDelta : 0;

    // Freeze app height while the keyboard is opening/open so the whole UI doesn't jump.
    const appHeight = keyboardInTransition ? lastNonKeyboardHeight : measuredHeight;

    // Safe bottom is intended for persistent device UI (home indicator / Android nav),
    // not the transient keyboard. When the keyboard is open, keep it at 0 so the
    // composer can sit close to the keyboard via --dc-keyboard-height.
    let safeBottom = 0;
    if (!keyboardInTransition) {
      const vvBottomInset = vv ? Math.max(0, layoutHeight - vvHeight - offsetTop) : 0;
      const clientBottomInset = Math.max(0, layoutHeight - clientHeight);
      safeBottom = Math.max(0, vvBottomInset, clientBottomInset);

      // Android (esp. with 3-button navigation) can report 0 insets even when the OS
      // nav overlaps the bottom of the layout. Keep a conservative minimum in mobile.
      if (IS_ANDROID && isMobileViewport()) {
        // 48px is a practical baseline for Android's system navigation area on many devices.
        safeBottom = Math.max(safeBottom, 48);
      }
    }

    root.style.setProperty("--dc-app-height", `${Math.round(appHeight)}px`);
    root.style.setProperty("--dc-safe-bottom", `${Math.round(safeBottom)}px`);
    root.style.setProperty("--dc-keyboard-height", `${Math.round(keyboardHeight)}px`);
  };

  let viewportRaf = null;
  const scheduleViewportVars = () => {
    if (viewportRaf) return;
    viewportRaf = window.requestAnimationFrame(() => {
      viewportRaf = null;
      updateViewportVars();
    });
  };

  updateViewportVars();
  window.addEventListener("resize", scheduleViewportVars, { passive: true });
  if (window.visualViewport) {
    window.visualViewport.addEventListener("resize", scheduleViewportVars);
    window.visualViewport.addEventListener("scroll", scheduleViewportVars);
  }
  // Some mobile browsers don't emit a reliable resize sequence for keyboard transitions.
  // Focus events are a cheap extra signal to recompute vars.
  let focusKickTimers = [];
  document.addEventListener("focusin", () => {
    scheduleViewportVars();
    // Kick a couple extra measurements during the keyboard animation.
    focusKickTimers.forEach((t) => clearTimeout(t));
    focusKickTimers = [
      setTimeout(scheduleViewportVars, 60),
      setTimeout(scheduleViewportVars, 180),
      setTimeout(scheduleViewportVars, 360),
    ];
  }, true);
  document.addEventListener("focusout", () => {
    scheduleViewportVars();
    focusKickTimers.forEach((t) => clearTimeout(t));
    focusKickTimers = [];
  }, true);

  const sidebarToggleBtn = document.getElementById("sidebarToggle");
  const sidebarFloatingToggleBtn = document.getElementById("sidebarFloatingToggle");
  const sidebarFloatingLoginBtn = document.getElementById("sidebarFloatingLogin");
  const sidebarAccountMenuWrapperEl = document.getElementById("sidebarAccountMenuWrapper");
  const sidebarAccountButtonEl = document.getElementById("sidebarAccountButton");
  const sidebarAccountMenuEl = document.getElementById("sidebarAccountMenu");

  const isMobileSidebar = isMobileViewport;

  const isUserAudience = () => {
    const aud = String(document.body?.dataset?.aud || "").trim().toLowerCase();
    return aud === "user" && !window.location.pathname.startsWith("/business");
  };

  const hasAuthToken = () => {
    try {
      const token = localStorage.getItem(LS_TOKEN);
      return Boolean(String(token || "").trim());
    } catch {
      return false;
    }
  };

  let floatingLoginDismissed = false;

  const refreshFloatingLogin = () => {
    if (!document.body) return;
    const shouldShow =
      isUserAudience()
      && isMobileSidebar()
      && !floatingLoginDismissed
      && !hasAuthToken()
      && document.body.dataset.sidebarCollapsed === "true";
    if (shouldShow) {
      document.body.dataset.floatingLogin = "true";
    } else {
      delete document.body.dataset.floatingLogin;
    }
  };

  const applySidebarCollapsed = (collapsed, { persist = true } = {}) => {
    const isCollapsed = Boolean(collapsed);
    if (document.body) {
      document.body.dataset.sidebarCollapsed = isCollapsed ? "true" : "false";
    }
    const btns = [sidebarToggleBtn, sidebarFloatingToggleBtn].filter(Boolean);
    btns.forEach((btn) => {
      const label = isCollapsed ? "Open sidebar" : "Close sidebar";
      btn.setAttribute("aria-expanded", isCollapsed ? "false" : "true");
      btn.setAttribute("aria-label", label);
      btn.dataset.tooltip = label;
    });
    if (persist) {
      try {
        localStorage.setItem(LS_SIDEBAR_COLLAPSED, isCollapsed ? "true" : "false");
      } catch {
        // ignore storage errors
      }
    }

    refreshFloatingLogin();
  };

  const loadSidebarCollapsedState = () => {
    // Deterministic initial state:
    // - Mobile: start collapsed
    // - Non-mobile: start expanded
    // Avoid persisting on load so desktop/mobile don't fight via shared storage.
    applySidebarCollapsed(isMobileSidebar(), { persist: false });
  };

  const setAccountMenuOpen = (open) => {
    if (!sidebarAccountMenuWrapperEl || !sidebarAccountButtonEl) return;
    const show = Boolean(open);
    sidebarAccountMenuWrapperEl.classList.toggle("open", show);
    sidebarAccountButtonEl.setAttribute("aria-expanded", show ? "true" : "false");
  };

  loadSidebarCollapsedState();

  const handleToggleClick = () => {
    const collapsed = document.body?.dataset?.sidebarCollapsed === "true";
    const nextCollapsed = !collapsed;
    if (collapsed && !nextCollapsed) {
      // Once the sidebar has been opened, hide the floating login shortcut for this page load.
      floatingLoginDismissed = true;
    }
    applySidebarCollapsed(nextCollapsed);
  };

  sidebarToggleBtn?.addEventListener("click", handleToggleClick);
  sidebarFloatingToggleBtn?.addEventListener("click", handleToggleClick);

  sidebarFloatingLoginBtn?.addEventListener("click", () => {
    if (!isUserAudience()) return;
    if (hasAuthToken()) return;
    if (typeof globalThis.creatureosUI?.showAuthModal === "function") {
      globalThis.creatureosUI.showAuthModal("login");
      return;
    }
    const loginBtn = document.getElementById("login-btn");
    if (loginBtn) {
      loginBtn.click();
      return;
    }
    // Worst-case fallback: reveal the sidebar so sign-in controls are visible.
    applySidebarCollapsed(false);
  });

  sidebarAccountButtonEl?.addEventListener("click", (event) => {
    event.stopPropagation();
    const isOpen = sidebarAccountMenuWrapperEl?.classList?.contains("open") ?? false;
    setAccountMenuOpen(!isOpen);
  });

  sidebarAccountButtonEl?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      const isOpen = sidebarAccountMenuWrapperEl?.classList?.contains("open") ?? false;
      setAccountMenuOpen(!isOpen);
    }
  });

  sidebarAccountMenuEl?.addEventListener("click", (event) => {
    const target = event.target?.closest?.("button, a");
    if (!target) return;
    setAccountMenuOpen(false);
  });

  document.addEventListener("click", (event) => {
    if (!sidebarAccountMenuWrapperEl) return;
    if (!sidebarAccountMenuWrapperEl.contains(event.target)) {
      setAccountMenuOpen(false);
    }
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setAccountMenuOpen(false);
    }
  });

  const confirmModalEl = document.querySelector("[data-confirm-modal-shell]");
  const confirmTitleEl = confirmModalEl?.querySelector("#agentConfirmTitle") ?? null;
  const confirmMessageEl = confirmModalEl?.querySelector("#agentConfirmMessage") ?? null;
  const confirmAcceptBtn = confirmModalEl?.querySelector("[data-confirm-accept-button]") ?? null;
  const confirmCancelBtn = confirmModalEl?.querySelector("[data-confirm-cancel-button]") ?? null;
  const confirmBackdropEl = confirmModalEl?.querySelector("[data-confirm-backdrop]") ?? null;
  let pendingConfirmForm = null;
  let pendingConfirmSubmitter = null;
  let confirmReturnFocusEl = null;

  const closeConfirmModal = ({ restoreFocus = true } = {}) => {
    if (!confirmModalEl) return;
    confirmModalEl.hidden = true;
    confirmModalEl.setAttribute("aria-hidden", "true");
    if (document.body) {
      delete document.body.dataset.confirmModalOpen;
    }
    pendingConfirmForm = null;
    pendingConfirmSubmitter = null;
    const focusTarget = confirmReturnFocusEl;
    confirmReturnFocusEl = null;
    if (restoreFocus && focusTarget instanceof HTMLElement) {
      window.requestAnimationFrame(() => {
        focusTarget.focus({ preventScroll: true });
      });
    }
  };

  const openConfirmModal = (form, submitter) => {
    if (!confirmModalEl) return;
    pendingConfirmForm = form;
    pendingConfirmSubmitter = submitter instanceof HTMLElement ? submitter : null;
    confirmReturnFocusEl = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    if (confirmTitleEl) {
      confirmTitleEl.textContent = String(form.dataset.confirmTitle || "Confirm action");
    }
    if (confirmMessageEl) {
      confirmMessageEl.textContent = String(form.dataset.confirmMessage || "Are you sure you want to continue?");
    }
    if (confirmAcceptBtn) {
      confirmAcceptBtn.textContent = String(form.dataset.confirmAccept || "Confirm");
    }
    if (confirmCancelBtn) {
      confirmCancelBtn.textContent = String(form.dataset.confirmCancel || "Cancel");
    }
    confirmModalEl.hidden = false;
    confirmModalEl.setAttribute("aria-hidden", "false");
    if (document.body) {
      document.body.dataset.confirmModalOpen = "true";
    }
    window.requestAnimationFrame(() => {
      (confirmCancelBtn || confirmAcceptBtn)?.focus({ preventScroll: true });
    });
  };

  if (confirmModalEl) {
    document.querySelectorAll("form[data-confirm-modal][onsubmit]").forEach((form) => {
      form.removeAttribute("onsubmit");
    });

    document.addEventListener("submit", (event) => {
      const form = event.target;
      if (!(form instanceof HTMLFormElement) || !form.matches("form[data-confirm-modal]")) {
        return;
      }
      if (form.dataset.confirmBypass === "true") {
        delete form.dataset.confirmBypass;
        return;
      }
      event.preventDefault();
      openConfirmModal(form, event.submitter);
    }, true);

    confirmAcceptBtn?.addEventListener("click", () => {
      const form = pendingConfirmForm;
      const submitter = pendingConfirmSubmitter;
      closeConfirmModal({ restoreFocus: false });
      if (!(form instanceof HTMLFormElement)) return;
      form.dataset.confirmBypass = "true";
      if (typeof form.requestSubmit === "function") {
        if (submitter instanceof HTMLElement) {
          form.requestSubmit(submitter);
        } else {
          form.requestSubmit();
        }
        return;
      }
      form.submit();
    });

    confirmCancelBtn?.addEventListener("click", () => {
      closeConfirmModal();
    });

    confirmBackdropEl?.addEventListener("click", () => {
      closeConfirmModal();
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !confirmModalEl.hidden) {
        event.preventDefault();
        closeConfirmModal();
      }
    });
  }

  // Expose a small API for other scripts (e.g. chat navigation) to control the sidebar.
  // Business Console templates may not load this file, so callers should handle the
  // function being unavailable.
  globalThis.creatureosShell = globalThis.creatureosShell || {};
  globalThis.creatureosShell.setSidebarCollapsed = applySidebarCollapsed;
  globalThis.creatureosShell.toggleSidebar = handleToggleClick;
  globalThis.creatureosShell.isSidebarCollapsed = () => document.body?.dataset?.sidebarCollapsed === "true";
  globalThis.creatureosShell.refreshFloatingLogin = refreshFloatingLogin;
})();
