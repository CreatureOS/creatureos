(() => {
  const escapeHtml = (value) => String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");

  const renderSimpleMarkdown = (markdown) => {
    const lines = String(markdown || "").replaceAll("\r\n", "\n").split("\n");
    const html = [];
    let listType = "";
    let paragraph = [];

    const inline = (value) => escapeHtml(value)
      .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
      .replace(/`([^`]+)`/g, "<code>$1</code>")
      .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');

    const flushParagraph = () => {
      if (!paragraph.length) return;
      html.push(`<p>${inline(paragraph.join(" "))}</p>`);
      paragraph = [];
    };

    const closeList = () => {
      if (!listType) return;
      html.push(listType === "ol" ? "</ol>" : "</ul>");
      listType = "";
    };

    lines.forEach((rawLine) => {
      const line = String(rawLine || "");
      const trimmed = line.trim();
      const bulletMatch = /^\s*[-*+]\s+(.+)$/.exec(line);
      const orderedMatch = /^\s*\d+\.\s+(.+)$/.exec(line);
      if (!trimmed) {
        flushParagraph();
        closeList();
        return;
      }
      if (bulletMatch) {
        flushParagraph();
        if (listType !== "ul") {
          closeList();
          html.push("<ul>");
          listType = "ul";
        }
        html.push(`<li>${inline(bulletMatch[1])}</li>`);
        return;
      }
      if (orderedMatch) {
        flushParagraph();
        if (listType !== "ol") {
          closeList();
          html.push("<ol>");
          listType = "ol";
        }
        html.push(`<li>${inline(orderedMatch[1])}</li>`);
        return;
      }
      closeList();
      paragraph.push(trimmed);
    });

    flushParagraph();
    closeList();
    return html.join("");
  };

  const cloneOnboardingSummonBlock = () => {
    const template = document.querySelector("[data-onboarding-summon-template]");
    if (!(template instanceof HTMLTemplateElement)) return null;
    return template.content.cloneNode(true);
  };

  const chatViewportRoots = (target = null) => {
    const roots = [];
    const globalChat = document.getElementById("chat");
    if (globalChat instanceof HTMLElement) {
      roots.push(globalChat);
    }
    if (target instanceof HTMLElement) {
      const localChatRoot = target.closest("#chat");
      if (localChatRoot instanceof HTMLElement && !roots.includes(localChatRoot)) {
        roots.push(localChatRoot);
      }
    }
    return roots;
  };

  const isChatViewportNearBottom = (target = null, threshold = 64) => (
    chatViewportRoots(target).some((root) => (root.scrollHeight - root.clientHeight - root.scrollTop) <= threshold)
  );

  const scrollChatViewportToBottom = (target = null) => {
    chatViewportRoots(target).forEach((root) => {
      root.scrollTop = root.scrollHeight;
    });
  };

  const scheduleChatViewportAutoScroll = (target = null) => {
    const run = () => scrollChatViewportToBottom(target);
    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(() => {
        run();
        window.requestAnimationFrame(run);
      });
      return;
    }
    window.setTimeout(run, 0);
  };

  const pinChatViewportWhile = (predicate, target = null) => {
    if (typeof predicate !== "function") {
      return () => {};
    }
    const roots = chatViewportRoots(target);
    if (!roots.length || !isChatViewportNearBottom(target)) {
      return () => {};
    }
    let stopped = false;
    let shouldFollow = true;
    let suppressScroll = false;

    const cleanup = () => {
      if (stopped) return;
      stopped = true;
      roots.forEach((root) => {
        root.removeEventListener("scroll", handleScroll);
      });
    };

    function handleScroll(event) {
      if (suppressScroll || stopped) return;
      const root = event.currentTarget;
      if (!(root instanceof HTMLElement)) return;
      const distanceFromBottom = root.scrollHeight - root.clientHeight - root.scrollTop;
      if (distanceFromBottom > 64) {
        shouldFollow = false;
        cleanup();
      }
    }

    roots.forEach((root) => {
      root.addEventListener("scroll", handleScroll, { passive: true });
    });

    const tick = () => {
      if (stopped) return;
      if (!predicate()) {
        cleanup();
        if (shouldFollow) {
          scheduleChatViewportAutoScroll(target);
        }
        return;
      }
      suppressScroll = true;
      scrollChatViewportToBottom(target);
      suppressScroll = false;
      if (typeof window.requestAnimationFrame === "function") {
        window.requestAnimationFrame(tick);
      } else {
        window.setTimeout(tick, 16);
      }
    };
    tick();
    return () => {
      const allowFinalScroll = shouldFollow;
      cleanup();
      if (allowFinalScroll) {
        scrollChatViewportToBottom(target);
      }
    };
  };

  const renderMarkdownIntoTarget = (target) => {
    if (!(target instanceof HTMLElement)) return;
    const source = target.parentElement?.querySelector("[data-markdown-source]");
    const markdown = String(source?.textContent || "");
    try {
      if (globalThis.marked && globalThis.DOMPurify) {
        target.innerHTML = globalThis.DOMPurify.sanitize(globalThis.marked.parse(markdown), {
          USE_PROFILES: { html: true },
        });
      } else {
        target.innerHTML = renderSimpleMarkdown(markdown);
      }
      target.querySelectorAll("pre code").forEach((block) => {
        try {
          globalThis.hljs?.highlightElement(block);
        } catch {}
      });
    } catch {
      target.innerHTML = renderSimpleMarkdown(markdown);
    }
    target.classList.remove("creature-markdown-content--pending");
    if (isChatViewportNearBottom(target)) {
      scheduleChatViewportAutoScroll(target);
    }
  };

  document.querySelectorAll("[data-markdown-target]").forEach((target) => {
    renderMarkdownIntoTarget(target);
  });

  const chatEl = document.getElementById("chat");
  const conversationListEl = document.getElementById("chatList");
  const activeView = String(document.body?.dataset?.activeView || "").trim().toLowerCase();
  const currentEcosystem = String(document.body?.dataset?.ecosystem || "woodlands").trim();
  const runFeedIdleLabel = "Listening";
  const runFeedActiveLabel = "Working";

  const runFeedLabelForStatus = (status) => (
    String(status || "").trim().toLowerCase() === "running" ? runFeedActiveLabel : runFeedIdleLabel
  );

  const liveRunFeedSummaryPanels = new Set();

  const composeWorkingSummaryLabel = (startedAt) => {
    const safeStartedAt = Number(startedAt || Date.now());
    const frame = Math.floor(Date.now() / 320) % 7;
    const dots = ".".repeat(frame + 1);
    return `${runFeedActiveLabel}${dots} ${formatFeedElapsed(Date.now() - safeStartedAt)}`;
  };

  const setRunFeedSummaryLabel = (target, status) => {
    const labelEl = target instanceof HTMLElement && target.matches("[data-run-feed-label]")
      ? target
      : target?.querySelector?.("[data-run-feed-label]");
    if (!(labelEl instanceof HTMLElement)) return;
    const panel = target instanceof HTMLDetailsElement
      ? target
      : labelEl.closest(".creature-run-feed");
    const normalized = String(status || "").trim().toLowerCase();
    if (panel instanceof HTMLElement && normalized) {
      panel.dataset.runStatus = normalized;
    }
    if (normalized === "running" && panel instanceof HTMLElement) {
      let startedAt = Number(panel.dataset.runSummaryStartedAt || "0");
      if (!Number.isFinite(startedAt) || startedAt <= 0) {
        startedAt = Date.now();
        panel.dataset.runSummaryStartedAt = String(startedAt);
      }
      liveRunFeedSummaryPanels.add(panel);
      labelEl.textContent = composeWorkingSummaryLabel(startedAt);
      return;
    }
    if (panel instanceof HTMLElement) {
      delete panel.dataset.runSummaryStartedAt;
      liveRunFeedSummaryPanels.delete(panel);
    }
    labelEl.textContent = runFeedLabelForStatus(status);
  };

  const syncRunFeedExpandedState = (panel, status) => {
    if (!(panel instanceof HTMLDetailsElement)) return;
    void status;
  };

  const initOnboardingEcosystemAssetWarmup = () => {
    const manifestEl = document.getElementById("onboardingEcosystemAssetManifest");
    if (!(manifestEl instanceof HTMLScriptElement)) {
      return {
        warmEcosystem: () => {},
      };
    }

    let manifest = {};
    try {
      const parsed = JSON.parse(String(manifestEl.textContent || "{}"));
      if (parsed && typeof parsed === "object") {
        manifest = parsed;
      }
    } catch {}

    const warmRequests = new Map();

    const warmUrl = (url, { priority = false } = {}) => {
      const href = String(url || "").trim();
      if (!href) return Promise.resolve();
      if (warmRequests.has(href)) {
        return warmRequests.get(href);
      }

      if (priority && !document.head.querySelector(`link[rel="preload"][as="image"][href="${href}"]`)) {
        const preload = document.createElement("link");
        preload.rel = "preload";
        preload.as = "image";
        preload.href = href;
        document.head.appendChild(preload);
      }

      const image = new Image();
      const request = new Promise((resolve) => {
        const finish = () => resolve();
        image.addEventListener("load", finish, { once: true });
        image.addEventListener("error", finish, { once: true });
        image.src = href;
        if (image.complete) {
          finish();
        }
      });
      warmRequests.set(href, request);
      return request;
    };

    const warmEcosystem = (ecosystemValue, options = {}) => {
      const key = String(ecosystemValue || "").trim();
      const urls = Array.isArray(manifest[key]) ? manifest[key] : [];
      urls.forEach((url) => {
        void warmUrl(url, options);
      });
    };

    warmEcosystem(currentEcosystem, { priority: true });

    const idleWarm = () => {
      Object.keys(manifest).forEach((ecosystemValue) => {
        if (ecosystemValue === currentEcosystem) return;
        warmEcosystem(ecosystemValue);
      });
    };

    if (typeof window.requestIdleCallback === "function") {
      window.requestIdleCallback(idleWarm, { timeout: 1200 });
    } else {
      window.setTimeout(idleWarm, 180);
    }

    return { warmEcosystem };
  };

  const onboardingEcosystemAssetWarmup = initOnboardingEcosystemAssetWarmup();

  const initEcosystemConfirmModal = () => {
    const modal = document.querySelector("[data-confirm-modal]");
    const backdrop = modal?.querySelector?.("[data-confirm-backdrop]");
    const card = modal?.querySelector?.("[data-confirm-card]");
    const kicker = modal?.querySelector?.("[data-confirm-kicker]");
    const title = modal?.querySelector?.("[data-confirm-title]");
    const body = modal?.querySelector?.("[data-confirm-body]");
    const cancelButton = modal?.querySelector?.("[data-confirm-cancel]");
    const confirmButton = modal?.querySelector?.("[data-confirm-accept]");

    if (
      !(modal instanceof HTMLElement) ||
      !(backdrop instanceof HTMLElement) ||
      !(card instanceof HTMLElement) ||
      !(kicker instanceof HTMLElement) ||
      !(title instanceof HTMLElement) ||
      !(body instanceof HTMLElement) ||
      !(cancelButton instanceof HTMLButtonElement) ||
      !(confirmButton instanceof HTMLButtonElement)
    ) {
      return async (options = {}) => window.confirm(String(options.message || options.body || "Are you sure?"));
    }

    let resolveConfirm = null;
    let restoreFocus = null;

    const getFocusable = () => Array.from(card.querySelectorAll(
      "button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])",
    )).filter((node) => node instanceof HTMLElement && !node.hidden);

    const close = (confirmed) => {
      if (typeof resolveConfirm !== "function") return;
      modal.hidden = true;
      delete document.body.dataset.confirmOpen;

      const nextResolve = resolveConfirm;
      const focusTarget = restoreFocus;
      resolveConfirm = null;
      restoreFocus = null;

      nextResolve(Boolean(confirmed));

      if (!confirmed && focusTarget instanceof HTMLElement) {
        requestAnimationFrame(() => {
          focusTarget.focus();
        });
      }
    };

    modal.addEventListener("keydown", (event) => {
      if (modal.hidden) return;
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopPropagation();
        close(false);
        return;
      }
      if (event.key !== "Tab") return;

      const focusable = getFocusable();
      if (!focusable.length) {
        event.preventDefault();
        card.focus();
        return;
      }

      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      const active = document.activeElement;
      if (event.shiftKey && active === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    });

    document.addEventListener("focusin", (event) => {
      if (modal.hidden || typeof resolveConfirm !== "function") return;
      if (event.target instanceof Node && card.contains(event.target)) return;
      const focusable = getFocusable();
      (focusable[0] || card).focus();
    });

    backdrop.addEventListener("click", () => {
      close(false);
    });

    cancelButton.addEventListener("click", () => {
      close(false);
    });

    confirmButton.addEventListener("click", () => {
      close(true);
    });

    return (options = {}) => {
      if (typeof resolveConfirm === "function") {
        close(false);
      }

      const variant = String(options.variant || "default").trim().toLowerCase();
      const nextVariant = variant === "danger" ? "danger" : "default";
      const nextKicker = String(
        options.kicker || (nextVariant === "danger" ? "Destructive action" : "Confirm action"),
      ).trim() || "Confirm action";
      const nextTitle = String(options.title || "Are you sure?").trim() || "Are you sure?";
      const nextBody = String(options.message || options.body || "").trim();
      const nextCancelLabel = String(options.cancelLabel || "Cancel").trim() || "Cancel";
      const nextConfirmLabel = String(options.confirmLabel || "Continue").trim() || "Continue";

      kicker.textContent = nextKicker;
      title.textContent = nextTitle;
      body.textContent = nextBody;
      cancelButton.textContent = nextCancelLabel;
      confirmButton.textContent = nextConfirmLabel;
      modal.dataset.variant = nextVariant;
      confirmButton.classList.toggle("creature-action-btn-danger", nextVariant === "danger");

      modal.hidden = false;
      document.body.dataset.confirmOpen = "true";
      restoreFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;

      return new Promise((resolve) => {
        resolveConfirm = resolve;
        requestAnimationFrame(() => {
          confirmButton.focus();
        });
      });
    };
  };

  const showEcosystemConfirm = initEcosystemConfirmModal();

  const submitFormWithConfirmBypass = (form, submitter) => {
    form.dataset.confirmBypass = "true";
    if (submitter instanceof HTMLButtonElement || submitter instanceof HTMLInputElement) {
      submitter.click();
      return;
    }
    if (typeof form.requestSubmit === "function") {
      form.requestSubmit();
      return;
    }
    form.submit();
  };

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;

    const message = String(form.dataset.confirmMessage || "").trim();
    if (!message) return;

    if (form.dataset.confirmBypass === "true") {
      delete form.dataset.confirmBypass;
      return;
    }

    event.preventDefault();

    const submitter = event.submitter;
    void showEcosystemConfirm({
      title: String(form.dataset.confirmTitle || "Are you sure?").trim() || "Are you sure?",
      message,
      confirmLabel: String(form.dataset.confirmConfirmLabel || "Continue").trim() || "Continue",
      cancelLabel: String(form.dataset.confirmCancelLabel || "Cancel").trim() || "Cancel",
      kicker: String(form.dataset.confirmKicker || "").trim(),
      variant: String(form.dataset.confirmVariant || "default").trim() || "default",
    }).then((confirmed) => {
      if (!confirmed) return;
      submitFormWithConfirmBypass(form, submitter);
    });
  });

  const setFeedStatusState = (statusEl, status) => {
    if (!(statusEl instanceof HTMLElement)) return;
    const nextStatus = String(status || "idle").trim() || "idle";
    statusEl.textContent = nextStatus;
    statusEl.dataset.state = nextStatus;
  };

  const buildWaitingLines = (ecosystem) => {
    const catalogs = {
      woodlands: [
        "Fireflies are pinning the first names to the moss board.",
        "A lantern is swinging over the first role sketches.",
        "Fresh tracks are crossing the clearing while the first creature takes shape.",
        "The den map is filling in with who should watch what.",
        "The tree line is deciding who wakes first.",
        "Someone is laying out the first satchels and responsibilities.",
        "The grove is matching woodland instincts to real work.",
        "Bark marks are turning into the first reliable creature draft.",
        "A patient branch is holding the opening introductions in place.",
        "The first watchposts are being claimed one by one.",
        "The woods are testing which names actually belong here.",
        "A quiet trail is turning into the first working paths.",
        "The clearing is sorting useful creatures from mere noise.",
        "The first campfire list is settling into shape.",
      ],
      "monster-wilds": [
        "The hoard is being distilled into one creature and its first priorities.",
        "A cliffside ledger is deciding which monsters wake first.",
        "The watchfire is sorting brutal ideas from useful ones.",
        "Someone is naming the first specialists beside the lair mouth.",
        "The brood line is tightening around one cleaner opening creature.",
        "A basalt perch is weighing who guards, who builds, and who scouts.",
        "The bestiary table is matching monsters to the work that matters.",
        "The roost map is turning instincts into assignments.",
        "Stormlight is catching the first creature as it takes form.",
        "The hoard shelf is filling with names that actually earn their place.",
        "A cavern wall is collecting the first working roles in chalk.",
        "The lair is deciding who should wake loud and who should stay watchful.",
        "The first hunt plan is being divided into creatures and lanes.",
        "A creature shape is hardening like cooling obsidian.",
      ],
      boneyard: [
        "Lantern light is sorting the first names at the gate.",
        "The fog is thinning around the opening creature.",
        "A crypt ledger is deciding who should rise first.",
        "Headstones are gathering the first duties into one place.",
        "The mausoleum table is turning whispers into real roles.",
        "Something useful is being pulled out of the dark and given a name.",
        "The first haunt is being arranged with more care than drama.",
        "A bell tower note is mapping who keeps watch and who investigates.",
        "The veil is separating eerie noise from creatures worth waking.",
        "A barrow path is filling with the first introductions.",
        "The boneyard line is deciding what belongs above ground first.",
        "The first circle is tightening around one carefully chosen creature.",
        "Someone is writing the opening roles where the fog can still read them.",
        "The grave ledger is turning restless signals into a plan.",
      ],
      sea: [
        "The tide chart is filling with the first creature names.",
        "A reef ledger is sorting who should surface first.",
        "Currents are carrying the opening creature into place.",
        "The shoal map is dividing the first work by instinct and depth.",
        "Someone is sounding the water for the cleanest starter roles.",
        "The harbor light is guiding the first useful creatures in.",
        "Kelp-side notes are turning drift into structure.",
        "The current is sorting helpers from distraction.",
        "A cove table is matching sea creatures to the work ahead.",
        "The first wave is being shaped carefully instead of noisily.",
        "The deepwater chart is marking who explores and who steadies the reef.",
        "Shell markers are lining up the opening introductions.",
        "The waterline is deciding what should stay quiet and what should surface.",
        "A shoal is forming around the strongest first roles.",
      ],
      expanse: [
        "A relay is assembling the first clean creature draft.",
        "The signal field is resolving into names and assignments.",
        "An orbit ledger is deciding who should come online first.",
        "A survey ring is filtering static out of the first creature draft.",
        "The landing grid is matching strange instincts to useful work.",
        "The first wave is being sequenced for a cleaner touchdown.",
        "A low satellite glow is marking the opening specialists.",
        "The transit rail is routing the first creatures into place.",
        "An observatory board is translating drift into responsibilities.",
        "The docking map is sorting explorers from stabilizers.",
        "A cluster of candidate names is tightening into one real creature.",
        "The expanse is deciding what belongs in first orbit.",
        "A first-contact slate is being revised into something sharper.",
        "Transmission lanes are being assigned to the creatures best suited to them.",
      ],
      terminal: [
        "The first boot sequence is assigning names to functions.",
        "A clean terminal is sorting useful creatures from extra noise.",
        "The relay grid is compiling the opening creature.",
        "A phosphor trace is resolving into roles and priorities.",
        "The startup graph is deciding what should come online first.",
        "A quiet rack is matching creatures to the jobs they can actually help with.",
        "The machine room is laying out the first working lanes.",
        "A watchdog list is trimming the first creature into shape.",
        "The signal board is routing responsibilities to the right creatures.",
        "The first stack is being assembled one clean role at a time.",
        "Console notes are turning vague intent into specific helpers.",
        "A shell prompt is collecting the first reliable names.",
        "The array is deciding what to boot now and what can wait.",
        "The opening creature is being packed with the right instincts.",
      ],
    };
    return Array.isArray(catalogs[ecosystem]) ? catalogs[ecosystem] : catalogs.boneyard;
  };

  const entryFromFeedLine = (line) => {
    const text = String(line || "").trim();
    if (!text) return null;
    const splitKinds = [
      { prefix: "Prepared update · ", kind: "draft", title: "Prepared update" },
      { prefix: "Prepared reply · ", kind: "draft", title: "Prepared reply" },
      { prefix: "Warning · ", kind: "warning", title: "Warning" },
      { prefix: "Error · ", kind: "warning", title: "Error" },
      { prefix: "Run started · ", kind: "status", title: "Run started" },
      { prefix: "Command failed · ", kind: "warning", title: "Command failed" },
    ];
    for (const item of splitKinds) {
      if (text.startsWith(item.prefix)) {
        return {
          kind: item.kind,
          title: item.title,
          detail: text.slice(item.prefix.length).trim(),
        };
      }
    }
    if (text === "Thread ready") return { kind: "thread", title: text, detail: "" };
    if (text === "Thinking through the next step") return { kind: "thinking", title: text, detail: "" };
    if (text === "Run completed") return { kind: "done", title: text, detail: "" };
    if (text === "Run failed") return { kind: "warning", title: text, detail: "" };
    return { kind: "note", title: text, detail: "" };
  };

  const normalizeFeedEntries = (payload = {}) => {
    const explicitEntries = Array.isArray(payload?.entries)
      ? payload.entries
          .map((item) => {
            if (!item || typeof item !== "object") return null;
            const title = String(item.title || "").trim();
            const detail = String(item.detail || "").trim();
            const kind = String(item.kind || "note").trim() || "note";
            if (!title && !detail) return null;
            return { kind, title, detail };
          })
          .filter(Boolean)
      : [];
    if (explicitEntries.length) {
      return explicitEntries;
    }
    return Array.isArray(payload?.lines)
      ? payload.lines
          .map((item) => entryFromFeedLine(item))
          .filter(Boolean)
      : [];
  };

  const renderStructuredFeedOutput = (feedOutput, payload = {}) => {
    if (!(feedOutput instanceof HTMLElement)) return;
    const entries = normalizeFeedEntries(payload);
    feedOutput.textContent = "";
    if (!entries.length) {
      const placeholder = document.createElement("div");
      placeholder.className = "onboarding-feed-placeholder";
      placeholder.textContent = "Waiting for first output…";
      feedOutput.appendChild(placeholder);
      feedOutput.dataset.waitingPlaceholder = "true";
      return;
    }
    delete feedOutput.dataset.waitingPlaceholder;
    const list = document.createElement("div");
    list.className = "onboarding-feed-list";
    entries.forEach((item) => {
      const entry = document.createElement("div");
      entry.className = `onboarding-feed-item onboarding-feed-item--${item.kind || "note"}`;
      const title = document.createElement("div");
      title.className = "onboarding-feed-item-title";
      title.textContent = item.title || item.detail || "";
      entry.appendChild(title);
      if (item.detail) {
        const detail = document.createElement("div");
        detail.className = "onboarding-feed-item-detail";
        detail.textContent = item.detail;
        entry.appendChild(detail);
      }
      list.appendChild(entry);
    });
    feedOutput.appendChild(list);
    feedOutput.scrollTop = feedOutput.scrollHeight;
  };

  const renderTextFeedOutput = (feedOutput, lines = [], placeholder = "Waiting for first output...") => {
    if (!(feedOutput instanceof HTMLElement)) return;
    const normalizedLines = Array.isArray(lines)
      ? lines.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    feedOutput.textContent = normalizedLines.join("\n");
    if (!normalizedLines.length) {
      feedOutput.textContent = placeholder;
      feedOutput.dataset.waitingPlaceholder = "true";
    } else {
      delete feedOutput.dataset.waitingPlaceholder;
    }
    feedOutput.scrollTop = feedOutput.scrollHeight;
  };

  const liveThinkingFeedState = new WeakMap();
  const liveThinkingFeedOutputs = new Set();

  const formatFeedElapsed = (elapsedMs) => {
    const seconds = Math.max(0, Math.floor(Number(elapsedMs || 0) / 1000));
    if (seconds < 60) {
      return `${seconds}s`;
    }
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = String(seconds % 60).padStart(2, "0");
    return `${minutes}:${remainingSeconds}`;
  };

  const shouldShowLiveThinkingLine = (status, lines = []) => {
    if (String(status || "").trim().toLowerCase() !== "running") return false;
    const loweredLines = Array.isArray(lines)
      ? lines.map((item) => String(item || "").trim().toLowerCase()).filter(Boolean)
      : [];
    if (!loweredLines.length) return false;
    const hasTurnStarted = loweredLines.some((line) => line === "turn started" || line === "thinking through the next step");
    if (!hasTurnStarted) return false;
    const hasTurnCompleted = loweredLines.some(
      (line) => line.startsWith("turn completed") || line === "run completed" || line === "run failed",
    );
    return !hasTurnCompleted;
  };

  const composeLiveThinkingLine = (startedAt) => {
    const safeStartedAt = Number(startedAt || Date.now());
    const frame = Math.floor(Date.now() / 320) % 7;
    const dots = ".".repeat(frame + 1);
    return `thinking${dots} ${formatFeedElapsed(Date.now() - safeStartedAt)}`;
  };

  const syncLiveThinkingFeedOutput = (feedOutput) => {
    if (!(feedOutput instanceof HTMLElement)) return;
    const state = liveThinkingFeedState.get(feedOutput);
    if (!state) return;

    const lines = Array.isArray(state.lines) ? [...state.lines] : [];
    const shouldShowThinkingLine = shouldShowLiveThinkingLine(state.status, lines);
    let nextStartedAt = Number(state.startedAt || 0);

    if (shouldShowThinkingLine) {
      if (!Number.isFinite(nextStartedAt) || nextStartedAt <= 0) {
        nextStartedAt = Date.now();
      }
      lines.push(composeLiveThinkingLine(nextStartedAt));
      liveThinkingFeedOutputs.add(feedOutput);
    } else {
      nextStartedAt = 0;
      liveThinkingFeedOutputs.delete(feedOutput);
    }

    liveThinkingFeedState.set(feedOutput, {
      ...state,
      startedAt: nextStartedAt,
    });

    renderTextFeedOutput(feedOutput, lines, state.placeholder);
  };

  const updateLiveThinkingFeed = (feedOutput, { lines = [], status = "idle", placeholder = "Waiting for first output..." } = {}) => {
    if (!(feedOutput instanceof HTMLElement)) return;
    const previousState = liveThinkingFeedState.get(feedOutput) || {};
    liveThinkingFeedState.set(feedOutput, {
      lines: Array.isArray(lines) ? [...lines] : [],
      status: String(status || "idle").trim() || "idle",
      placeholder: String(placeholder || "Waiting for first output..."),
      startedAt: Number(previousState.startedAt || 0),
    });
    syncLiveThinkingFeedOutput(feedOutput);
  };

  window.setInterval(() => {
    liveThinkingFeedOutputs.forEach((feedOutput) => {
      syncLiveThinkingFeedOutput(feedOutput);
    });
    liveRunFeedSummaryPanels.forEach((panel) => {
      setRunFeedSummaryLabel(panel, panel instanceof HTMLElement ? String(panel.dataset.runStatus || "idle") : "idle");
    });
  }, 320);

  let startTypewriterOnNode = () => false;

  const initTypewriterText = () => {
    const collectRenderableTextNodes = (root) => {
      if (!(root instanceof HTMLElement)) return [];
      const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
        acceptNode(textNode) {
          const parent = textNode.parentElement;
          if (!parent) return NodeFilter.FILTER_REJECT;
          if (["SCRIPT", "STYLE"].includes(parent.tagName)) return NodeFilter.FILTER_REJECT;
          if (!String(textNode.textContent || "").trim()) return NodeFilter.FILTER_REJECT;
          return NodeFilter.FILTER_ACCEPT;
        },
      });
      const nodes = [];
      let nextNode = walker.nextNode();
      while (nextNode) {
        nodes.push({
          node: nextNode,
          text: String(nextNode.textContent || ""),
        });
        nextNode = walker.nextNode();
      }
      return nodes;
    };

    const syncStructuredVisibility = (root) => {
      if (!(root instanceof HTMLElement)) return;
      const blocks = Array.from(root.querySelectorAll("p, li, ul, ol, blockquote, h1, h2, h3, h4, h5, h6, form"));
      blocks.reverse().forEach((element) => {
        if (!(element instanceof HTMLElement)) return;
        const tag = element.tagName;
        let visible = false;
        if (tag === "UL" || tag === "OL") {
          visible = Array.from(element.children).some((child) => child instanceof HTMLElement && !child.hidden);
        } else if (tag === "FORM") {
          visible = Boolean(String(element.textContent || "").trim());
        } else {
          visible = Boolean(String(element.textContent || "").trim());
          if (!visible) {
            visible = Array.from(element.children).some((child) => child instanceof HTMLElement && !child.hidden);
          }
        }
        element.hidden = !visible;
      });
    };

    const runStructuredMarkdownTypewriter = (node, speed, finalize) => {
      if (!(node instanceof HTMLElement)) return false;
      renderMarkdownIntoTarget(node);
      const textNodes = collectRenderableTextNodes(node);
      if (!textNodes.length) {
        finalize();
        return true;
      }

      textNodes.forEach((entry) => {
        entry.node.textContent = "";
      });

      const totalChars = textNodes.reduce((sum, entry) => sum + entry.text.length, 0);
      let cursor = 0;

      const paint = () => {
        let remaining = cursor;
        textNodes.forEach((entry) => {
          const visibleChars = Math.max(0, Math.min(entry.text.length, remaining));
          entry.node.textContent = entry.text.slice(0, visibleChars);
          remaining -= visibleChars;
        });
        syncStructuredVisibility(node);
      };

      const tick = () => {
        cursor += 1;
        paint();
        if (cursor >= totalChars) {
          finalize();
          return;
        }
        window.setTimeout(tick, speed);
      };

      paint();
      window.setTimeout(tick, speed);
      return true;
    };

    const finalizeTypewriterText = (node, fullText) => {
      if (!(node instanceof HTMLElement)) return;
      if (node.dataset.typewriterMarkdown === "true") {
        renderMarkdownIntoTarget(node);
      } else {
        node.textContent = fullText;
      }
    };

    startTypewriterOnNode = (node, delayMs = 0) => {
      if (!(node instanceof HTMLElement)) return;
      const fullText = String(node.dataset.typewriterText || node.textContent || "").trim();
      if (!fullText) return false;
      const onceKey = String(node.dataset.typewriterOnceKey || "").trim();
      const onceStorageKey = onceKey ? `creatureos:typewriter:${onceKey}` : "";
      if (onceStorageKey) {
        try {
          if (window.localStorage.getItem(onceStorageKey) === "done") {
            finalizeTypewriterText(node, fullText);
            node.classList.remove("is-typing");
            return true;
          }
        } catch {}
      }
      const speed = Math.max(5, Number(node.dataset.typewriterSpeed || 8));
      node.classList.add("is-typing");
      const stopPinning = pinChatViewportWhile(() => node.classList.contains("is-typing"), node);

      const finish = () => {
        finalizeTypewriterText(node, fullText);
        node.classList.remove("is-typing");
        stopPinning();
        if (onceStorageKey) {
          try {
            window.localStorage.setItem(onceStorageKey, "done");
          } catch {}
        }
      };

      const tick = () => {
        if (node.dataset.typewriterMarkdown === "true") {
          if (runStructuredMarkdownTypewriter(node, speed, finish)) {
            return;
          }
        }

        node.textContent = "";
        let cursor = 0;
        const plainTick = () => {
          cursor += 1;
          node.textContent = fullText.slice(0, cursor);
          if (cursor >= fullText.length) {
            finish();
            return;
          }
          window.setTimeout(plainTick, speed);
        };
        window.setTimeout(plainTick, speed);
      };

      window.setTimeout(tick, delayMs);
      return true;
    };

    document.querySelectorAll("[data-typewriter-text]").forEach((node, index) => {
      startTypewriterOnNode(node, index * 120);
    });
  };

  const resetShellScroll = () => {
    if (activeView === "chat") return;
    const scrollRoots = [
      document.scrollingElement,
      document.querySelector(".page"),
      document.querySelector(".page > .panel.panel-main"),
      document.querySelector(".page > .panel.panel-main > .panel-body"),
      document.querySelector(".panel.panel-sidebar > .panel-body"),
      document.querySelector("#chatView > .panel-body"),
    ];
    scrollRoots.forEach((node) => {
      if (!(node instanceof HTMLElement)) return;
      node.scrollTop = 0;
      node.scrollLeft = 0;
    });
  };

  resetShellScroll();
  requestAnimationFrame(resetShellScroll);
  window.setTimeout(resetShellScroll, 80);

  const initEcosystemTooltips = () => {
    document.querySelectorAll(".local-creature-app [title]").forEach((element) => {
      if (!(element instanceof HTMLElement)) return;
      const tooltip = String(element.getAttribute("title") || "").trim();
      if (!tooltip) return;
      if (!element.dataset.tooltip) {
        element.dataset.tooltip = tooltip;
      }
      element.removeAttribute("title");
    });
  };

  const initCreatureHeaderMenu = () => {
    const trigger = document.querySelector("[data-creature-header-menu-trigger]");
    const popover = document.querySelector("[data-creature-header-menu-popover]");
    if (!(trigger instanceof HTMLButtonElement) || !(popover instanceof HTMLElement)) {
      return { close: () => {} };
    }

    const close = ({ focus = false } = {}) => {
      popover.hidden = true;
      trigger.setAttribute("aria-expanded", "false");
      if (focus) {
        try { trigger.focus({ preventScroll: true }); } catch {}
      }
    };

    const open = () => {
      popover.hidden = false;
      trigger.setAttribute("aria-expanded", "true");
      const first = popover.querySelector("button, [href], input, select, textarea");
      if (first instanceof HTMLElement) {
        try { first.focus({ preventScroll: true }); } catch {}
      }
    };

    const toggle = () => {
      if (popover.hidden) {
        open();
        return;
      }
      close();
    };

    trigger.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      toggle();
    });

    popover.addEventListener("click", (event) => {
      const menuItem = event.target?.closest?.(".creature-header-menu-item");
      if (!menuItem) return;
      close();
    });

    document.addEventListener("click", (event) => {
      const target = event.target;
      if (target?.closest?.("[data-creature-header-menu-popover]") || target?.closest?.("[data-creature-header-menu-trigger]")) {
        return;
      }
      close();
    });

    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape" || popover.hidden) return;
      event.preventDefault();
      close({ focus: true });
    });

    return { close, open };
  };

  const initCreatureSheetModals = (menuState) => {
    const modalEls = Array.from(document.querySelectorAll("[data-creature-sheet-modal]")).filter(
      (modalEl) => modalEl instanceof HTMLElement,
    );
    if (!modalEls.length) return;

    let activeModal = null;
    let restoreFocus = null;

    const getFocusable = (modalEl) => Array.from(modalEl.querySelectorAll(
      "button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex='-1'])",
    )).filter((node) => node instanceof HTMLElement && !node.hidden);

    const close = ({ focus = true } = {}) => {
      if (!(activeModal instanceof HTMLElement)) return;
      activeModal.hidden = true;
      delete document.body.dataset.creatureSheetOpen;
      const target = restoreFocus;
      activeModal = null;
      restoreFocus = null;
      if (focus && target instanceof HTMLElement) {
        requestAnimationFrame(() => {
          try { target.focus({ preventScroll: true }); } catch {}
        });
      }
    };

    const open = (modalName, triggerEl = null) => {
      const modalEl = modalEls.find((candidate) => candidate.dataset.creatureSheetModal === modalName);
      if (!(modalEl instanceof HTMLElement)) return;
      modalEls.forEach((candidate) => {
        candidate.hidden = candidate !== modalEl;
      });
      menuState?.close?.();
      activeModal = modalEl;
      restoreFocus = triggerEl instanceof HTMLElement ? triggerEl : document.activeElement;
      modalEl.hidden = false;
      document.body.dataset.creatureSheetOpen = "true";
      requestAnimationFrame(() => {
        const preferred = modalEl.querySelector("[data-creature-sheet-autofocus]");
        const first = preferred instanceof HTMLElement ? preferred : getFocusable(modalEl)[0];
        if (first instanceof HTMLElement) {
          try { first.focus({ preventScroll: true }); } catch {}
          if (first instanceof HTMLInputElement || first instanceof HTMLTextAreaElement) {
            first.select();
          }
        }
      });
    };

    document.addEventListener("click", (event) => {
      const openButton = event.target?.closest?.("[data-creature-modal-open]");
      if (openButton instanceof HTMLElement) {
        event.preventDefault();
        event.stopPropagation();
        open(String(openButton.dataset.creatureModalOpen || "").trim(), openButton);
        return;
      }
      const closeButton = event.target?.closest?.("[data-creature-sheet-close]");
      if (closeButton instanceof HTMLElement) {
        event.preventDefault();
        close();
      }
    });

    document.addEventListener("keydown", (event) => {
      if (!(activeModal instanceof HTMLElement)) return;
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopPropagation();
        close();
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = getFocusable(activeModal);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      const active = document.activeElement;
      if (event.shiftKey && active === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    });

    document.addEventListener("focusin", (event) => {
      if (!(activeModal instanceof HTMLElement)) return;
      if (event.target instanceof Node && activeModal.contains(event.target)) return;
      const focusable = getFocusable(activeModal);
      (focusable[0] || activeModal).focus();
    });

    modalEls.forEach((modalEl) => {
      const autosaveForm = modalEl.querySelector("[data-creature-sheet-autosave-form]");
      if (!(autosaveForm instanceof HTMLFormElement)) return;
      let submitting = false;
      autosaveForm.querySelectorAll("[data-creature-sheet-autosave-on='change']").forEach((fieldEl) => {
        fieldEl.addEventListener("change", async () => {
          if (submitting) return;
          submitting = true;
          try {
            const response = await fetch(autosaveForm.action, {
              method: "POST",
              headers: { "x-creatureos-ajax": "1" },
              body: new FormData(autosaveForm),
            });
            if (!response.ok) {
              throw new Error(`Autosave failed with status ${response.status}`);
            }
            close({ focus: false });
          } catch (error) {
            console.error(error);
          } finally {
            submitting = false;
          }
        });
      });
    });
  };

  const initCustomSelects = () => {
    const customSelects = [];

    const maybeSubmitAutosaveForField = (fieldEl) => {
      if (!(fieldEl instanceof HTMLElement)) return;
      if (String(fieldEl.dataset.autosaveOn || "").trim() !== "change") return;
      const formEl = fieldEl.closest("form[data-autosave-form]");
      const submitAutosave = formEl && typeof formEl.__creatureosAutosaveSubmit === "function"
        ? formEl.__creatureosAutosaveSubmit
        : null;
      if (typeof submitAutosave === "function") {
        void submitAutosave();
      }
    };

    const setSelectLayerState = (selectState, isOpen) => {
      if (!selectState) return;
      const parents = [
        selectState.shell.closest(".dc-box"),
        selectState.shell.closest(".creature-settings-form"),
        selectState.shell.closest(".creature-settings-inline-form"),
        selectState.shell.closest(".creature-compose-shell"),
      ];
      parents.forEach((parentEl) => {
        if (!(parentEl instanceof HTMLElement)) return;
        parentEl.classList.toggle("has-open-select", Boolean(isOpen));
      });
    };

    const closeCustomSelect = (selectState) => {
      if (!selectState) return;
      selectState.shell.classList.remove("is-open");
      selectState.menu.hidden = true;
      selectState.trigger.setAttribute("aria-expanded", "false");
      setSelectLayerState(selectState, false);
    };

    const closeAllCustomSelects = (exceptShell = null) => {
      customSelects.forEach((selectState) => {
        if (exceptShell && selectState.shell === exceptShell) return;
        closeCustomSelect(selectState);
      });
    };

    const buildOptionButton = ({ optionEl, selectState, menuFragment }) => {
      if (!(optionEl instanceof HTMLOptionElement) || optionEl.disabled) return;
      const optionButton = document.createElement("button");
      optionButton.type = "button";
      optionButton.className = "creature-select-option";
      optionButton.setAttribute("role", "option");
      optionButton.dataset.value = optionEl.value;
      optionButton.textContent = optionEl.textContent || optionEl.label || optionEl.value;
      optionButton.addEventListener("click", () => {
        if (selectState.select.value !== optionEl.value) {
          selectState.select.value = optionEl.value;
          selectState.select.dispatchEvent(new Event("input", { bubbles: true }));
          selectState.select.dispatchEvent(new Event("change", { bubbles: true }));
          maybeSubmitAutosaveForField(selectState.select);
        }
        closeCustomSelect(selectState);
        selectState.trigger.focus();
      });
      menuFragment.appendChild(optionButton);
    };

    document.querySelectorAll(".local-creature-app select").forEach((selectEl) => {
      if (!(selectEl instanceof HTMLSelectElement) || selectEl.dataset.customSelectReady === "true") return;
      selectEl.dataset.customSelectReady = "true";
      const selectVariant = String(selectEl.dataset.customSelectVariant || "").trim().toLowerCase();

      const shell = document.createElement("div");
      shell.className = "creature-select-shell";
      if (selectVariant) {
        shell.classList.add(`creature-select-shell--${selectVariant}`);
      }

      const trigger = document.createElement("button");
      trigger.type = "button";
      trigger.className = "creature-select-trigger";
      trigger.setAttribute("aria-haspopup", "listbox");
      trigger.setAttribute("aria-expanded", "false");
      const triggerLabel = String(selectEl.getAttribute("aria-label") || selectEl.name || "Select option").trim();
      if (triggerLabel) {
        trigger.setAttribute("aria-label", triggerLabel);
      }

      const valueEl = document.createElement("span");
      valueEl.className = "creature-select-value";
      const arrowEl = document.createElement("span");
      arrowEl.className = "creature-select-arrow";
      arrowEl.setAttribute("aria-hidden", "true");
      trigger.append(valueEl, arrowEl);

      const menu = document.createElement("div");
      menu.className = "creature-select-menu";
      if (selectVariant) {
        menu.classList.add(`creature-select-menu--${selectVariant}`);
      }
      menu.hidden = true;
      menu.setAttribute("role", "listbox");
      if (selectEl.name) {
        menu.id = `creature-select-menu-${selectEl.name}-${customSelects.length + 1}`;
        trigger.setAttribute("aria-controls", menu.id);
      }

      const selectState = { select: selectEl, shell, trigger, menu, valueEl };
      const enabledOptions = () => Array.from(selectEl.options).filter((optionEl) => !optionEl.disabled);

      const syncFromSelect = () => {
        const selectedOption = selectEl.options[selectEl.selectedIndex] || enabledOptions()[0] || null;
        valueEl.textContent = selectedOption?.textContent || selectedOption?.label || "";
        menu.querySelectorAll(".creature-select-option").forEach((optionButtonEl) => {
          if (!(optionButtonEl instanceof HTMLButtonElement)) return;
          const isSelected = optionButtonEl.dataset.value === selectEl.value;
          optionButtonEl.classList.toggle("is-selected", isSelected);
          optionButtonEl.setAttribute("aria-selected", isSelected ? "true" : "false");
        });
        shell.classList.toggle("is-disabled", selectEl.disabled);
        trigger.disabled = selectEl.disabled;
      };

      const openCustomSelect = () => {
        if (selectEl.disabled) return;
        closeAllCustomSelects(shell);
        shell.classList.add("is-open");
        menu.hidden = false;
        trigger.setAttribute("aria-expanded", "true");
        setSelectLayerState(selectState, true);
        const selectedOptionEl = menu.querySelector(".creature-select-option.is-selected") || menu.querySelector(".creature-select-option");
        if (selectedOptionEl instanceof HTMLElement) {
          selectedOptionEl.scrollIntoView({ block: "nearest" });
        }
      };

      const moveSelection = (delta) => {
        const options = enabledOptions();
        if (!options.length) return;
        const currentIndex = Math.max(0, options.findIndex((optionEl) => optionEl.value === selectEl.value));
        const nextIndex = Math.max(0, Math.min(options.length - 1, currentIndex + delta));
        const nextOption = options[nextIndex];
        if (!nextOption) return;
        if (selectEl.value !== nextOption.value) {
          selectEl.value = nextOption.value;
          selectEl.dispatchEvent(new Event("input", { bubbles: true }));
          selectEl.dispatchEvent(new Event("change", { bubbles: true }));
          maybeSubmitAutosaveForField(selectEl);
        }
      };

      const menuFragment = document.createDocumentFragment();
      Array.from(selectEl.children).forEach((childEl) => {
        if (childEl instanceof HTMLOptionElement) {
          buildOptionButton({ optionEl: childEl, selectState, menuFragment });
          return;
        }
        if (!(childEl instanceof HTMLOptGroupElement)) return;
        const groupEl = document.createElement("div");
        groupEl.className = "creature-select-group";
        const groupLabelEl = document.createElement("div");
        groupLabelEl.className = "creature-select-group-label";
        groupLabelEl.textContent = childEl.label;
        groupEl.appendChild(groupLabelEl);
        Array.from(childEl.children).forEach((optionEl) => buildOptionButton({ optionEl, selectState, menuFragment: groupEl }));
        menuFragment.appendChild(groupEl);
      });
      menu.appendChild(menuFragment);

      trigger.addEventListener("click", () => {
        if (menu.hidden) {
          openCustomSelect();
        } else {
          closeCustomSelect(selectState);
        }
      });

      trigger.addEventListener("keydown", (event) => {
        if (event.key === "ArrowDown" || event.key === "ArrowUp") {
          event.preventDefault();
          moveSelection(event.key === "ArrowDown" ? 1 : -1);
          return;
        }
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          if (menu.hidden) {
            openCustomSelect();
          } else {
            closeCustomSelect(selectState);
          }
          return;
        }
        if (event.key === "Escape") {
          closeCustomSelect(selectState);
        }
      });

      selectEl.addEventListener("change", syncFromSelect);
      selectEl.hidden = true;
      selectEl.classList.add("creature-select-native");
      shell.append(trigger, menu);
      selectEl.insertAdjacentElement("afterend", shell);
      syncFromSelect();
      customSelects.push(selectState);
    });

    document.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) {
        closeAllCustomSelects();
        return;
      }
      const shell = target.closest(".creature-select-shell");
      if (shell) {
        closeAllCustomSelects(shell);
        return;
      }
      closeAllCustomSelects();
    });

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        closeAllCustomSelects();
      }
    });

  };

  const initNumberSteppers = () => {
    const clampNumericValue = (inputEl, rawValue) => {
      const min = Number.parseFloat(inputEl.min);
      const max = Number.parseFloat(inputEl.max);
      const stepAttr = String(inputEl.step || "").trim();
      const step = stepAttr && stepAttr !== "any" ? Number.parseFloat(stepAttr) : 1;
      let nextValue = Number.isFinite(rawValue) ? rawValue : 0;
      if (Number.isFinite(min)) nextValue = Math.max(min, nextValue);
      if (Number.isFinite(max)) nextValue = Math.min(max, nextValue);
      const stepDecimals = Number.isFinite(step) ? ((String(step).split(".")[1] || "").length) : 0;
      if (stepDecimals > 0) {
        return nextValue.toFixed(stepDecimals).replace(/\.?0+$/, "");
      }
      return String(Math.round(nextValue));
    };

    document.querySelectorAll(".local-creature-app input[type='number']").forEach((inputEl) => {
      if (!(inputEl instanceof HTMLInputElement) || inputEl.dataset.numberStepperReady === "true" || inputEl.type === "hidden") return;
      inputEl.dataset.numberStepperReady = "true";

      const field = document.createElement("span");
      field.className = "creature-number-field";
      const controls = document.createElement("span");
      controls.className = "creature-number-stepper";

      const buildStepButton = (direction, label) => {
        const buttonEl = document.createElement("button");
        buttonEl.type = "button";
        buttonEl.className = "creature-number-stepper-btn";
        buttonEl.dataset.stepDirection = direction;
        buttonEl.setAttribute("aria-label", label);
        buttonEl.innerHTML = direction === "up"
          ? '<svg viewBox="0 0 12 12" aria-hidden="true" focusable="false"><path d="M2.25 7.25 6 3.5l3.75 3.75" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="1.35"/></svg>'
          : '<svg viewBox="0 0 12 12" aria-hidden="true" focusable="false"><path d="M2.25 4.75 6 8.5l3.75-3.75" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="1.35"/></svg>';
        return buttonEl;
      };

      const decrementButton = buildStepButton("down", "Decrease value");
      const incrementButton = buildStepButton("up", "Increase value");
      controls.append(incrementButton, decrementButton);

      const syncStepperDisabledState = () => {
        const currentValue = Number.parseFloat(inputEl.value);
        const min = Number.parseFloat(inputEl.min);
        const max = Number.parseFloat(inputEl.max);
        decrementButton.disabled = Number.isFinite(min) && Number.isFinite(currentValue) ? currentValue <= min : false;
        incrementButton.disabled = Number.isFinite(max) && Number.isFinite(currentValue) ? currentValue >= max : false;
      };

      const stepValue = (direction) => {
        const stepAttr = String(inputEl.step || "").trim();
        const step = stepAttr && stepAttr !== "any" ? Number.parseFloat(stepAttr) : 1;
        const min = Number.parseFloat(inputEl.min);
        const currentValue = Number.parseFloat(inputEl.value);
        const baseValue = Number.isFinite(currentValue) ? currentValue : (Number.isFinite(min) ? min : 0);
        const delta = direction === "up" ? step : -step;
        inputEl.value = clampNumericValue(inputEl, baseValue + delta);
        inputEl.dispatchEvent(new Event("input", { bubbles: true }));
        inputEl.dispatchEvent(new Event("change", { bubbles: true }));
      };

      decrementButton.addEventListener("click", () => stepValue("down"));
      incrementButton.addEventListener("click", () => stepValue("up"));
      inputEl.addEventListener("input", syncStepperDisabledState);
      inputEl.addEventListener("change", syncStepperDisabledState);

      const parent = inputEl.parentNode;
      if (!parent) return;
      parent.insertBefore(field, inputEl);
      field.append(inputEl, controls);
      syncStepperDisabledState();
    });
  };

  initEcosystemTooltips();
  initCustomSelects();
  initNumberSteppers();
  initTypewriterText();

  document.querySelectorAll("[data-autosave-form]").forEach((formEl) => {
    if (!(formEl instanceof HTMLFormElement)) return;
    let autosaveTimer = 0;
    let submitting = false;

    const submitAutosaveForm = async () => {
      if (submitting || !formEl.reportValidity()) return;
      const ownerReferenceSelect = formEl.querySelector("[data-owner-reference-select]");
      const ownerReferenceCustomInput = formEl.querySelector("input[name='owner_reference_custom']");
      if (ownerReferenceSelect instanceof HTMLSelectElement && ownerReferenceSelect.value === "__custom__") {
        const customValue = ownerReferenceCustomInput instanceof HTMLInputElement
          ? ownerReferenceCustomInput.value.trim()
          : "";
        if (!customValue) {
          if (ownerReferenceCustomInput instanceof HTMLInputElement) {
            requestAnimationFrame(() => ownerReferenceCustomInput.focus());
          }
          return;
        }
      }
      submitting = true;
      try {
        const response = await fetch(formEl.action, {
          method: String(formEl.method || "POST").toUpperCase(),
          headers: { "x-creatureos-ajax": "1" },
          body: new FormData(formEl),
        });
        if (!response.ok) {
          throw new Error(`Autosave failed with status ${response.status}`);
        }
        if (formEl.action.includes("/settings/ecosystem")) {
          const ecosystemField = formEl.querySelector("select[name='ecosystem_choice']");
          if (ecosystemField instanceof HTMLSelectElement) {
            document.body.dataset.ecosystem = String(ecosystemField.value || "").trim() || document.body.dataset.ecosystem;
          }
        }
      } catch (error) {
        console.error(error);
      } finally {
        submitting = false;
      }
    };

    formEl.__creatureosAutosaveSubmit = submitAutosaveForm;

    formEl.querySelectorAll("[data-autosave-on='change']").forEach((fieldEl) => {
      fieldEl.addEventListener("change", () => {
        void submitAutosaveForm();
      });
    });

    formEl.querySelectorAll("[data-autosave-on='input']").forEach((fieldEl) => {
      const scheduleAutosave = () => {
        window.clearTimeout(autosaveTimer);
        autosaveTimer = window.setTimeout(() => {
          void submitAutosaveForm();
        }, 450);
      };
      fieldEl.addEventListener("input", scheduleAutosave);
      fieldEl.addEventListener("change", () => {
        void submitAutosaveForm();
      });
    });
  });

  document.querySelectorAll("[data-owner-reference-select]").forEach((selectEl) => {
    const ownerReferenceSelect = selectEl;
    const form = ownerReferenceSelect.closest("form");
    const ownerReferenceCustomWrap = form?.querySelector?.("[data-owner-reference-custom-wrap]");
    const ownerReferenceCustomInput = ownerReferenceCustomWrap?.querySelector?.("input");
    const syncOwnerReferenceUi = () => {
      if (!(ownerReferenceSelect instanceof HTMLSelectElement) || !(ownerReferenceCustomWrap instanceof HTMLElement)) return;
      const useCustom = ownerReferenceSelect.value === "__custom__";
      ownerReferenceCustomWrap.hidden = !useCustom;
      if (useCustom && ownerReferenceCustomInput instanceof HTMLInputElement && document.activeElement === ownerReferenceSelect) {
        requestAnimationFrame(() => ownerReferenceCustomInput.focus());
      }
    };
    if (ownerReferenceSelect instanceof HTMLSelectElement) {
      ownerReferenceSelect.addEventListener("change", syncOwnerReferenceUi);
      syncOwnerReferenceUi();
    }
  });

  document.querySelectorAll("[data-onboarding-ecosystem-form]").forEach((formEl) => {
    if (!(formEl instanceof HTMLFormElement)) return;
    const hiddenInput = formEl.querySelector("[data-onboarding-ecosystem-input]");
    const ecosystemCards = formEl.querySelectorAll("[data-onboarding-ecosystem-option]");
    const applyEcosystemSelection = (ecosystemValue) => {
      const nextEcosystem = String(ecosystemValue || "").trim();
      if (!nextEcosystem) return;
      onboardingEcosystemAssetWarmup.warmEcosystem(nextEcosystem, { priority: true });
      if (hiddenInput instanceof HTMLInputElement) {
        hiddenInput.value = nextEcosystem;
      }
      document.body.dataset.ecosystem = nextEcosystem;
      ecosystemCards.forEach((cardEl) => {
        if (!(cardEl instanceof HTMLElement)) return;
        cardEl.classList.toggle("is-selected", cardEl.dataset.onboardingEcosystemOption === nextEcosystem);
      });
    };

    ecosystemCards.forEach((cardEl) => {
      if (!(cardEl instanceof HTMLButtonElement)) return;
      const warm = () => onboardingEcosystemAssetWarmup.warmEcosystem(cardEl.dataset.onboardingEcosystemOption || "");
      cardEl.addEventListener("pointerenter", warm);
      cardEl.addEventListener("focus", warm);
      cardEl.addEventListener("click", () => {
        applyEcosystemSelection(cardEl.dataset.onboardingEcosystemOption || "");
      });
    });

    if (hiddenInput instanceof HTMLInputElement) {
      applyEcosystemSelection(hiddenInput.value);
    }
  });

  const initOnboardingPanelToggle = () => {
    const toggleButtons = Array.from(document.querySelectorAll("[data-onboarding-panel-toggle]"))
      .filter((buttonEl) => buttonEl instanceof HTMLButtonElement);
    if (!toggleButtons.length) return;
    if (activeView !== "onboarding-ecosystem") return;

    const storageKey = "creatureos:onboarding-panel-hidden";
    let hidden = false;

    try {
      hidden = window.localStorage.getItem(storageKey) === "true";
    } catch {}

    const sync = () => {
      if (hidden) {
        document.body.dataset.onboardingPanelHidden = "true";
      } else {
        delete document.body.dataset.onboardingPanelHidden;
      }

      toggleButtons.forEach((buttonEl) => {
        const showLabel = String(buttonEl.dataset.showLabel || "Show chooser").trim() || "Show chooser";
        const hideLabel = String(buttonEl.dataset.hideLabel || "Hide chooser").trim() || "Hide chooser";
        const labelEl = buttonEl.querySelector(".nav-label");
        if (labelEl instanceof HTMLElement) {
          labelEl.textContent = hidden ? showLabel : hideLabel;
        }
        buttonEl.setAttribute("aria-pressed", hidden ? "true" : "false");
        buttonEl.hidden = false;
      });
    };

    toggleButtons.forEach((buttonEl) => {
      buttonEl.addEventListener("click", () => {
        hidden = !hidden;
        try {
          window.localStorage.setItem(storageKey, hidden ? "true" : "false");
        } catch {}
        sync();
      });
    });

    sync();
  };

  initOnboardingPanelToggle();

  if (chatEl) {
    chatEl.scrollTop = chatEl.scrollHeight;
  }

  document.querySelectorAll("[data-activity-post]").forEach((post) => {
    const body = post.querySelector("[data-activity-body]");
    const toggle = post.querySelector("[data-activity-toggle]");
    if (!(body instanceof HTMLElement) || !(toggle instanceof HTMLButtonElement)) return;

    const hasContent = body.textContent.trim().length > 0;
    if (!hasContent) {
      body.classList.remove("is-collapsed");
      toggle.hidden = true;
      return;
    }

    const setExpanded = (expanded) => {
      body.classList.toggle("is-collapsed", !expanded);
      post.classList.toggle("is-expanded", expanded);
      toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
      toggle.textContent = expanded ? "Hide details" : "Details";
    };

    setExpanded(post.dataset.expandInitial === "true");
    toggle.addEventListener("click", () => {
      setExpanded(toggle.getAttribute("aria-expanded") !== "true");
    });
  });

  const initOnboardingChat = () => {
    const form = document.querySelector("[data-onboarding-chat-form]");
    const input = form?.querySelector?.("[data-onboarding-input]");
    const rail = document.getElementById("chat-rail");
    const feedPanel = document.querySelector("[data-onboarding-run-feed]");
    const feedStatus = feedPanel?.querySelector?.("[data-onboarding-feed-status]");
    const feedOutput = feedPanel?.querySelector?.("[data-onboarding-feed-output]");
    const shell = form?.closest?.(".creature-compose-shell");
    const composer = shell?.closest?.(".onboarding-composer") || null;
    const uploadInput = form?.querySelector?.("[data-chat-upload-input]") || null;
    const uploadButton = shell?.querySelector?.("[data-chat-upload-button]") || null;
    const composeAttachmentsEl = form?.querySelector?.("[data-compose-attachments]") || null;
    const chatThinkingForm = shell?.querySelector?.("[data-chat-thinking-form]") || null;
    const chatThinkingNote = chatThinkingForm?.querySelector?.("[data-chat-thinking-note]") || null;
    if (!(form instanceof HTMLFormElement) || !(input instanceof HTMLTextAreaElement) || !(rail instanceof HTMLElement)) return;

    const submitButton = form.querySelector("button[type='submit']");
    let feedPollId = 0;
    let keepFeedVisible = false;
    let lastFeedPayload = { status: String(feedStatus?.textContent || "idle").trim() || "idle", lines: [] };
    let activeFeedSource = null;
    let activeFeedRunId = 0;

    const syncSummonButton = (ready) => {
      const slot = document.querySelector("[data-onboarding-summon-slot]");
      if (!(slot instanceof HTMLElement)) return;
      const existing = slot.querySelector(".onboarding-summon-wrap");
      if (ready) {
        slot.hidden = false;
        if (existing instanceof HTMLElement) return;
        const block = cloneOnboardingSummonBlock();
        if (!block) return;
        slot.append(block);
        scrollChatToBottom();
        return;
      }
      slot.hidden = true;
      if (existing instanceof HTMLElement) {
        existing.remove();
      }
    };

    const scrollChatToBottom = () => {
      scrollChatViewportToBottom();
      scheduleChatViewportAutoScroll();
    };

    const syncInputHeight = () => {
      input.style.height = "auto";
      const nextHeight = Math.min(Math.max(input.scrollHeight, 44), 220);
      input.style.height = `${nextHeight}px`;
      input.style.overflowY = input.scrollHeight > 220 ? "auto" : "hidden";
    };

    const formatBytesCompact = (value) => {
      const numeric = Number(value || 0);
      if (!Number.isFinite(numeric) || numeric <= 0) return "";
      const units = ["B", "KB", "MB", "GB"];
      let size = numeric;
      let unitIndex = 0;
      while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex += 1;
      }
      if (unitIndex === 0) {
        return `${Math.round(size)} ${units[unitIndex]}`;
      }
      const rounded = Math.round(size * 10) / 10;
      return `${String(rounded).replace(/\\.0$/, "")} ${units[unitIndex]}`;
    };

    const selectedUploadFiles = () => {
      if (!(uploadInput instanceof HTMLInputElement) || !uploadInput.files) return [];
      return Array.from(uploadInput.files);
    };

    const setUploadFiles = (files) => {
      if (!(uploadInput instanceof HTMLInputElement)) return;
      const transfer = new DataTransfer();
      for (const file of files) {
        if (file instanceof File) {
          transfer.items.add(file);
        }
      }
      uploadInput.files = transfer.files;
    };

    const mergeUploadFiles = (incomingFiles) => {
      const merged = [];
      const seen = new Set();
      for (const file of [...selectedUploadFiles(), ...incomingFiles]) {
        if (!(file instanceof File)) continue;
        const key = `${file.name}::${file.size}::${file.lastModified}::${file.type}`;
        if (seen.has(key)) continue;
        seen.add(key);
        merged.push(file);
      }
      setUploadFiles(merged);
    };

    const buildAttachmentPreviewNode = ({ name = "", sizeLabel = "", type = "", url = "" } = {}) => {
      const link = document.createElement(url ? "a" : "div");
      link.className = type.startsWith("image/") && url
        ? "creature-message-attachment creature-message-attachment--image"
        : "creature-message-attachment";
      if (url && link instanceof HTMLAnchorElement) {
        link.href = url;
        link.target = "_blank";
        link.rel = "noopener";
      }
      if (type.startsWith("image/") && url) {
        const image = document.createElement("img");
        image.src = url;
        image.alt = name;
        image.loading = "lazy";
        link.appendChild(image);
        const label = document.createElement("span");
        label.textContent = name;
        link.appendChild(label);
        return link;
      }
      const strong = document.createElement("strong");
      strong.textContent = name;
      const meta = document.createElement("span");
      meta.textContent = sizeLabel;
      link.append(strong, meta);
      return link;
    };

    const renderComposeAttachments = () => {
      if (!(composeAttachmentsEl instanceof HTMLElement)) return;
      const files = selectedUploadFiles();
      composeAttachmentsEl.textContent = "";
      composeAttachmentsEl.hidden = files.length === 0;
      files.forEach((file, index) => {
        const chip = document.createElement("div");
        chip.className = "creature-compose-attachment-chip";
        const summary = document.createElement("div");
        summary.className = "creature-compose-attachment-summary";
        const name = document.createElement("strong");
        name.textContent = file.name;
        const meta = document.createElement("span");
        meta.textContent = formatBytesCompact(file.size);
        summary.append(name, meta);
        const removeButton = document.createElement("button");
        removeButton.type = "button";
        removeButton.className = "creature-compose-attachment-remove";
        removeButton.textContent = "Remove";
        removeButton.dataset.attachmentIndex = String(index);
        chip.append(summary, removeButton);
        composeAttachmentsEl.appendChild(chip);
      });
    };

    const setChatThinkingNote = (text, state = "") => {
      if (!(chatThinkingNote instanceof HTMLElement)) return;
      const nextText = String(text || "").trim();
      chatThinkingNote.textContent = nextText;
      chatThinkingNote.dataset.state = state;
      chatThinkingNote.hidden = nextText.length === 0;
    };

    const appendMessageRow = (role, contentNode) => {
      const row = document.createElement("div");
      row.className = `row ${role}`;
      const bubble = document.createElement("div");
      bubble.className = role === "assistant" ? "bubble bubble--full-span" : "bubble";
      bubble.appendChild(contentNode);
      row.appendChild(bubble);
      rail.appendChild(row);
      scrollChatToBottom();
      return row;
    };

    const appendUserMessage = (text, attachments = []) => {
      const content = document.createElement("div");
      if (text) {
        const body = document.createElement("div");
        body.className = "creature-bubble-text";
        body.textContent = text;
        content.appendChild(body);
      }
      if (attachments.length) {
        const attachmentWrap = document.createElement("div");
        attachmentWrap.className = "creature-message-attachments";
        attachments.forEach((item) => {
          attachmentWrap.appendChild(buildAttachmentPreviewNode(item));
        });
        content.appendChild(attachmentWrap);
      }
      return appendMessageRow("user", content);
    };

    const appendAssistantMessage = (markdown) => {
      const source = document.createElement("script");
      source.type = "text/plain";
      source.className = "creature-markdown-source";
      source.dataset.markdownSource = "";
      source.textContent = String(markdown || "");

      const target = document.createElement("div");
      target.className = "creature-message-content creature-markdown-content--pending";
      target.dataset.markdownTarget = "";
      target.dataset.typewriterText = String(markdown || "");
      target.dataset.typewriterSpeed = "12";
      target.dataset.typewriterMarkdown = "true";

      const wrapper = document.createElement("div");
      wrapper.append(source, target);
      const row = appendMessageRow("assistant", wrapper);
      if (!startTypewriterOnNode(target)) {
        renderMarkdownIntoTarget(target);
      }
      return row;
    };

    const renderFeed = (payload = {}) => {
      if (!(feedPanel instanceof HTMLDetailsElement) || !(feedOutput instanceof HTMLElement)) return;
      lastFeedPayload = payload;
      const nextStatus = String(payload?.status || "idle").trim() || "idle";
      const nextLines = Array.isArray(payload?.lines)
        ? payload.lines.map((item) => String(item || "").trim()).filter(Boolean)
        : [];
      const shouldShow = keepFeedVisible || nextStatus === "running" || nextLines.length > 0;
      feedPanel.hidden = !shouldShow;
      syncRunFeedExpandedState(feedPanel, nextStatus);
      setRunFeedSummaryLabel(feedPanel, nextStatus);
      setFeedStatusState(feedStatus, nextStatus);
      updateLiveThinkingFeed(feedOutput, {
        lines: nextLines,
        status: nextStatus,
        placeholder: "Waiting for first output...",
      });
    };

    const stopFeedPolling = () => {
      if (!feedPollId) return;
      window.clearInterval(feedPollId);
      feedPollId = 0;
    };

    const closeFeedStream = () => {
      if (!(activeFeedSource instanceof EventSource)) return;
      activeFeedSource.close();
      activeFeedSource = null;
      activeFeedRunId = 0;
    };

    const maybeStartFeedStream = (payload = {}) => {
      const runId = Number.parseInt(String(payload?.run_id || "0"), 10);
      const streamUrl = String(payload?.stream_url || "").trim();
      const status = String(payload?.status || "").trim().toLowerCase();
      if (!streamUrl || !Number.isFinite(runId) || runId <= 0 || status !== "running") {
        return;
      }
      if (activeFeedSource instanceof EventSource && activeFeedRunId === runId) {
        return;
      }
      closeFeedStream();
      stopFeedPolling();
      const streamTarget = new URL(streamUrl, window.location.origin);
      const lastEventId = Number.parseInt(String(payload?.last_event_id || "0"), 10);
      if (Number.isFinite(lastEventId) && lastEventId > 0) {
        streamTarget.searchParams.set("last_event_id", String(lastEventId));
      }
      const source = new EventSource(streamTarget.toString());
      activeFeedSource = source;
      activeFeedRunId = runId;
      source.onmessage = (event) => {
        let streamPayload = {};
        try {
          streamPayload = JSON.parse(String(event.data || "{}"));
        } catch {}
        const nextLine = String(streamPayload?.display_body || "").trim();
        const mergedLines = Array.isArray(lastFeedPayload.lines) ? [...lastFeedPayload.lines] : [];
        if (nextLine && mergedLines[mergedLines.length - 1] !== nextLine) {
          mergedLines.push(nextLine);
        }
        lastFeedPayload = {
          ...lastFeedPayload,
          status: String(streamPayload?.run_status || lastFeedPayload.status || "running"),
          run_id: runId,
          stream_url: streamUrl,
          last_event_id: Number.parseInt(String(streamPayload?.event_id || payload?.last_event_id || "0"), 10) || 0,
          lines: mergedLines,
        };
        renderFeed(lastFeedPayload);
        const nextStatus = String(streamPayload?.run_status || "").trim().toLowerCase();
        if (nextStatus && nextStatus !== "running") {
          closeFeedStream();
        }
      };
      source.onerror = () => {
        closeFeedStream();
        if (form.dataset.submitting === "true") {
          startFeedPolling();
        }
      };
    };

    const fetchFeed = async () => {
      const response = await fetch("/onboarding/feed", {
        headers: { "x-creatureos-ajax": "1" },
      });
      if (!response.ok) {
        throw new Error(`Feed request failed with status ${response.status}`);
      }
      const payload = await response.json();
      const status = String(payload?.status || "").trim().toLowerCase();
      const runId = Number.parseInt(String(payload?.run_id || "0"), 10);
      const isSubmitting = form.dataset.submitting === "true";

      if (isSubmitting && (!Number.isFinite(runId) || runId <= 0) && status !== "running") {
        renderFeed({
          ...lastFeedPayload,
          status: "running",
          lines: Array.isArray(lastFeedPayload.lines) ? lastFeedPayload.lines : [],
        });
        return lastFeedPayload;
      }

      renderFeed(payload);
      maybeStartFeedStream(payload);
      if (!isSubmitting && status && status !== "running") {
        stopFeedPolling();
      }
      return payload;
    };

    const startFeedPolling = () => {
      keepFeedVisible = true;
      renderFeed({ status: "running", lines: [] });
      void fetchFeed().catch((error) => {
        console.error(error);
      });
      stopFeedPolling();
      feedPollId = window.setInterval(() => {
        void fetchFeed().catch((error) => {
          console.error(error);
        });
      }, 850);
    };

    const setSubmitting = (isSubmitting) => {
      input.disabled = isSubmitting;
      if (submitButton instanceof HTMLButtonElement) {
        submitButton.disabled = isSubmitting;
      }
      if (uploadButton instanceof HTMLButtonElement) {
        uploadButton.disabled = isSubmitting;
      }
      form.dataset.submitting = isSubmitting ? "true" : "false";
    };

    syncInputHeight();
    renderComposeAttachments();
    input.addEventListener("input", syncInputHeight);
    if (String(feedStatus?.textContent || "").trim() === "running") {
      startFeedPolling();
    }
    syncSummonButton(Boolean(document.querySelector(".onboarding-summon-wrap")));

    input.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" || event.shiftKey) return;
      event.preventDefault();
      if (!String(input.value || "").trim() && selectedUploadFiles().length === 0) return;
      if (submitButton instanceof HTMLButtonElement) {
        form.requestSubmit(submitButton);
        return;
      }
      form.requestSubmit();
    });

    if (uploadButton instanceof HTMLButtonElement && uploadInput instanceof HTMLInputElement) {
      uploadButton.addEventListener("click", () => {
        if (form.dataset.submitting === "true") return;
        uploadInput.click();
      });
      uploadInput.addEventListener("change", () => {
        renderComposeAttachments();
      });
    }

    composeAttachmentsEl?.addEventListener("click", (event) => {
      const removeButton = event.target?.closest?.("[data-attachment-index]");
      if (!(removeButton instanceof HTMLButtonElement)) return;
      const index = Number.parseInt(String(removeButton.dataset.attachmentIndex || "-1"), 10);
      if (!Number.isFinite(index) || index < 0) return;
      const remainingFiles = selectedUploadFiles().filter((_, itemIndex) => itemIndex !== index);
      setUploadFiles(remainingFiles);
      renderComposeAttachments();
    });

    input.addEventListener("paste", (event) => {
      const pastedFiles = Array.from(event.clipboardData?.items || [])
        .map((item) => (item.kind === "file" ? item.getAsFile() : null))
        .filter((file) => file instanceof File);
      if (!pastedFiles.length) return;
      event.preventDefault();
      mergeUploadFiles(pastedFiles);
      renderComposeAttachments();
    });

    if (chatThinkingForm instanceof HTMLFormElement) {
      let thinkingSaveTimer = 0;
      let thinkingSubmitting = false;
      const submitChatThinkingForm = async () => {
        if (thinkingSubmitting) return;
        thinkingSubmitting = true;
        setChatThinkingNote("Saving...", "saving");
        try {
          const response = await fetch(chatThinkingForm.action, {
            method: "POST",
            headers: { "x-creatureos-ajax": "1" },
            body: new FormData(chatThinkingForm),
          });
          if (!response.ok) {
            throw new Error(`Thinking save failed with status ${response.status}`);
          }
          const payload = await response.json();
          const model = String(payload?.model_label || payload?.model || "").trim();
          const effort = String(payload?.reasoning_effort_label || payload?.reasoning_effort || "").trim();
          setChatThinkingNote(`Saved · ${model}${effort ? ` · ${effort}` : ""}`, "saved");
          window.clearTimeout(thinkingSaveTimer);
          thinkingSaveTimer = window.setTimeout(() => {
            setChatThinkingNote("", "");
          }, 1800);
        } catch (error) {
          console.error(error);
          setChatThinkingNote("Could not save right now.", "error");
        } finally {
          thinkingSubmitting = false;
        }
      };

      chatThinkingForm.querySelectorAll("[data-chat-thinking-field]").forEach((fieldEl) => {
        fieldEl.addEventListener("change", () => {
          void submitChatThinkingForm();
        });
      });
    }

    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (form.dataset.submitting === "true") return;

      const message = String(input.value || "").trim();
      const pendingFiles = selectedUploadFiles();
      if (!message && pendingFiles.length === 0) return;

      const formData = new FormData(form);
      const pendingAttachments = pendingFiles.map((file) => ({
        name: file.name,
        sizeLabel: formatBytesCompact(file.size),
        type: file.type,
        url: file.type.startsWith("image/") ? URL.createObjectURL(file) : "",
      }));
      appendUserMessage(message, pendingAttachments);
      input.value = "";
      setUploadFiles([]);
      renderComposeAttachments();
      syncInputHeight();
      setSubmitting(true);
      startFeedPolling();

      let payload = null;
      try {
        const response = await fetch(form.action, {
          method: "POST",
          body: formData,
          headers: { "x-creatureos-ajax": "1" },
        });
        const responseText = await response.text();
        let parsedPayload = {};
        if (responseText) {
          try {
            parsedPayload = JSON.parse(responseText);
          } catch {
            parsedPayload = {};
          }
        }
        if (!response.ok) {
          const detail = String(parsedPayload?.detail || "").trim();
          throw new Error(detail || `Request failed with status ${response.status}`);
        }
        payload = parsedPayload;
      } catch (error) {
        const content = document.createElement("div");
        content.className = "creature-bubble-text";
        content.textContent = "Send failed. Refresh and retry if the Keeper does not answer.";
        appendMessageRow("system", content);
        input.value = message;
        setUploadFiles(pendingFiles);
        renderComposeAttachments();
        syncInputHeight();
        await fetchFeed().catch((feedError) => {
          console.error(feedError);
        });
        keepFeedVisible = false;
        renderFeed(lastFeedPayload);
        setSubmitting(false);
        input.focus();
        console.error(error);
        return;
      }

      const assistantBody = String(payload?.assistant_body || "I lost the thread. Try again and I’ll pick it back up.");
      try {
        appendAssistantMessage(assistantBody);
      } catch (error) {
        console.error(error);
        const content = document.createElement("div");
        content.className = "creature-bubble-text";
        content.textContent = assistantBody;
        appendMessageRow("assistant", content);
      }
      syncSummonButton(Boolean(payload?.starter_ready));
      await fetchFeed().catch((error) => {
        console.error(error);
      });
      keepFeedVisible = false;
      renderFeed(lastFeedPayload);
      setSubmitting(false);
      input.focus();
    });

    window.addEventListener("pagehide", () => {
      stopFeedPolling();
      closeFeedStream();
    });
  };

  initOnboardingChat();

  const initOnboardingSummonWait = () => {
    const waitModal = document.querySelector("[data-onboarding-wait-modal]");
    const waitLine = waitModal?.querySelector?.("[data-onboarding-wait-line]");
    if (!(waitModal instanceof HTMLElement)) return;

    const lines = buildWaitingLines(currentEcosystem);
    let lineIndex = 0;
    let timerId = 0;
    let activeSummonForm = null;

    const stopLineCycle = () => {
      if (!timerId) return;
      window.clearInterval(timerId);
      timerId = 0;
    };

    const showModal = () => {
      waitModal.hidden = false;
      stopLineCycle();
      lineIndex = Math.floor(Math.random() * lines.length);
      if (waitLine instanceof HTMLElement) {
        waitLine.textContent = lines[lineIndex] || "Preparing a name, purpose, and introduction…";
      }
      timerId = window.setInterval(() => {
        lineIndex = (lineIndex + 1) % lines.length;
        if (waitLine instanceof HTMLElement) {
          waitLine.textContent = lines[lineIndex];
        }
      }, 5000);
    };

    document.addEventListener("submit", async (event) => {
      const summonForm = event.target;
      if (!(summonForm instanceof HTMLFormElement) || !summonForm.hasAttribute("data-onboarding-starter-form")) return;
      if (event.defaultPrevented) return;
      const summonButton = summonForm.querySelector("[data-onboarding-summon-button]");
      if (!(summonButton instanceof HTMLButtonElement)) return;
      if (summonButton.disabled) {
        event.preventDefault();
        return;
      }
      if (summonForm.dataset.waitingSubmit === "true") {
        return;
      }
      event.preventDefault();
      activeSummonForm = summonForm;
      summonForm.dataset.waitingSubmit = "true";
      document.querySelectorAll("[data-onboarding-summon-button]").forEach((buttonEl) => {
        if (buttonEl instanceof HTMLButtonElement) {
          buttonEl.disabled = true;
        }
      });
      showModal();
      try {
        const response = await fetch(summonForm.action, {
          method: "POST",
          body: new FormData(summonForm),
          headers: { "x-creatureos-ajax": "1" },
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw new Error(String(payload?.detail || `Summoning the first creature failed with status ${response.status}`));
        }
        window.location.assign(String(payload?.redirect_url || "/"));
      } catch (error) {
        stopLineCycle();
        summonForm.dataset.waitingSubmit = "false";
        activeSummonForm = null;
        document.querySelectorAll("[data-onboarding-summon-button]").forEach((buttonEl) => {
          if (buttonEl instanceof HTMLButtonElement) {
            buttonEl.disabled = false;
          }
        });
        if (waitLine instanceof HTMLElement) {
          waitLine.textContent = "Summoning hit a problem. Refresh and try again.";
        }
        console.error(error);
      }
    });

    window.addEventListener("pagehide", () => {
      stopLineCycle();
    });
  };

  initOnboardingSummonWait();
  const creatureHeaderMenuState = initCreatureHeaderMenu();
  initCreatureSheetModals(creatureHeaderMenuState);

  let sidebarAwakeningPollId = 0;
  const awakeningTransitionNotices = new Set(["starter-creatures-creating", "creature-creating", "creatures-creating"]);

  const currentConversationUrl = () => `${window.location.pathname}${window.location.search}`;
  const currentTransitionNotice = () => {
    try {
      return new URL(window.location.href).searchParams.get("notice") || "";
    } catch {
      return "";
    }
  };
  const shouldRefreshResolvedAwakeningPage = () => awakeningTransitionNotices.has(currentTransitionNotice());
  const resolvedAwakeningRedirectUrl = () => {
    try {
      const nextUrl = new URL(window.location.href);
      nextUrl.searchParams.delete("notice");
      return `${nextUrl.pathname}${nextUrl.search}${nextUrl.hash}`;
    } catch {
      return currentConversationUrl();
    }
  };

  const refreshSidebarSections = async (url) => {
    const targetUrl = String(url || currentConversationUrl()).trim();
    if (!targetUrl) return;
    const response = await fetch(targetUrl, {
      method: "GET",
      headers: { "x-creatureos-ajax": "1" },
    });
    if (!response.ok) {
      throw new Error(`Sidebar refresh failed with status ${response.status}`);
    }
    const html = await response.text();
    const parser = new DOMParser();
    const doc = parser.parseFromString(html, "text/html");
    const nextEcosystemNav = doc.querySelector(".ecosystem-nav");
    const currentEcosystemNav = document.querySelector(".ecosystem-nav");
    if (nextEcosystemNav instanceof HTMLElement && currentEcosystemNav instanceof HTMLElement) {
      currentEcosystemNav.innerHTML = nextEcosystemNav.innerHTML;
    }
    const nextChatList = doc.getElementById("chatList");
    if (nextChatList instanceof HTMLElement && conversationListEl instanceof HTMLElement) {
      conversationListEl.innerHTML = nextChatList.innerHTML;
    }
    syncSidebarAwakeningWatch();
  };

  const hasAwakeningSidebarRows = () => Boolean(
    document.querySelector(".ecosystem-creature-row--awakening, .creature-nav-status--awakening"),
  );

  const stopSidebarAwakeningWatch = () => {
    if (!sidebarAwakeningPollId) return;
    window.clearInterval(sidebarAwakeningPollId);
    sidebarAwakeningPollId = 0;
  };

  const startSidebarAwakeningWatch = () => {
    if (sidebarAwakeningPollId || !hasAwakeningSidebarRows()) return;
    sidebarAwakeningPollId = window.setInterval(async () => {
      if (!hasAwakeningSidebarRows()) {
        stopSidebarAwakeningWatch();
        return;
      }
      try {
        await refreshSidebarSections(window.location.href);
      } catch (error) {
        console.error(error);
      }
      if (!hasAwakeningSidebarRows()) {
        stopSidebarAwakeningWatch();
        if (shouldRefreshResolvedAwakeningPage()) {
          window.location.replace(resolvedAwakeningRedirectUrl());
        }
      }
    }, 1800);
  };

  function syncSidebarAwakeningWatch() {
    if (hasAwakeningSidebarRows()) {
      startSidebarAwakeningWatch();
      return;
    }
    stopSidebarAwakeningWatch();
  }

  syncSidebarAwakeningWatch();
  window.addEventListener("pagehide", stopSidebarAwakeningWatch);

  const form = document.querySelector(".creature-compose-form");
  const input = document.getElementById("input");
  const sendButton = document.getElementById("send");
  const rail = document.getElementById("chat-rail");
  const root = document.body;
  const uploadInput = form?.querySelector?.("[data-chat-upload-input]") || null;
  const uploadButton = document.querySelector("[data-chat-upload-button]") || null;
  const composeAttachmentsEl = form?.querySelector?.("[data-compose-attachments]") || null;
  const chatThinkingForm = document.querySelector("[data-chat-thinking-form]");
  const chatThinkingNote = chatThinkingForm?.querySelector?.("[data-chat-thinking-note]") || null;
  const submitButtons = Array.from(document.querySelectorAll(".creature-compose-form button[type='submit']")).filter(
    (buttonEl) => buttonEl instanceof HTMLButtonElement,
  );
  const defaultBusySubmit = form?.querySelector?.("[data-busy-default-submit]") || null;
  const MAX_CHAT_TITLE_CHARS = 120;
  if (!(form instanceof HTMLFormElement) || !(input instanceof HTMLTextAreaElement) || !(rail instanceof HTMLElement)) return;
  if (form.matches("[data-onboarding-chat-form]")) return;

  let openConversationMenuId = null;

  const closeConversationMenu = ({ focus = false } = {}) => {
    if (!conversationListEl) return;
    const open = conversationListEl.querySelector(".chat-nav-menu-popover:not([hidden])");
    if (!open) {
      openConversationMenuId = null;
      return;
    }
    const cid = String(open.dataset?.conversationId || "").trim();
    open.hidden = true;
    const button = cid
      ? conversationListEl.querySelector(`.chat-nav-menu[data-conversation-id="${cid}"]`)
      : null;
    if (button instanceof HTMLButtonElement) {
      button.setAttribute("aria-expanded", "false");
      if (focus) {
        try { button.focus({ preventScroll: true }); } catch {}
      }
    }
    openConversationMenuId = null;
  };

  const toggleConversationMenu = (conversationId) => {
    if (!conversationListEl) return;
    const cid = String(conversationId || "").trim();
    if (!cid) return;
    const menu = conversationListEl.querySelector(`.chat-nav-menu-popover[data-conversation-id="${cid}"]`);
    const button = conversationListEl.querySelector(`.chat-nav-menu[data-conversation-id="${cid}"]`);
    if (!(menu instanceof HTMLElement) || !(button instanceof HTMLButtonElement)) return;

    const isOpen = !menu.hidden;
    closeConversationMenu();
    if (isOpen) return;

    menu.hidden = false;
    button.setAttribute("aria-expanded", "true");
    openConversationMenuId = cid;
    const first = menu.querySelector(".chat-nav-menu-item");
    if (first instanceof HTMLElement) {
      try { first.focus({ preventScroll: true }); } catch {}
    }
  };

  const renameConversation = async (renameUrl, title) => {
    const body = new URLSearchParams({ title });
    const response = await fetch(renameUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "x-creatureos-ajax": "1",
      },
      body,
    });
    if (!response.ok) {
      throw new Error(`Rename failed with status ${response.status}`);
    }
    return response.json();
  };

  const deleteConversation = async (deleteUrl, currentConversationId) => {
    const body = new URLSearchParams();
    if (currentConversationId) {
      body.set("current_conversation_id", String(currentConversationId));
    }
    const response = await fetch(deleteUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "x-creatureos-ajax": "1",
      },
      body,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(payload?.detail || `Delete failed with status ${response.status}`);
    }
    return payload;
  };

  const beginRenameConversation = (conversationId, rowEl, renameUrl) => {
    const row = rowEl?.closest?.(".chat-nav-row") || rowEl;
    if (!(row instanceof HTMLElement)) return;
    const link = row.querySelector(".settings-nav-item");
    const label = link?.querySelector?.(".nav-label");
    if (!(link instanceof HTMLElement) || !(label instanceof HTMLElement)) return;

    const originalTitle = String(label.textContent || "").trim() || "New chat";
    const input = document.createElement("input");
    input.type = "text";
    input.className = "chat-rename-input";
    input.maxLength = MAX_CHAT_TITLE_CHARS;
    input.value = originalTitle === "New chat" ? "" : originalTitle;
    input.setAttribute("aria-label", "Chat title");

    label.replaceWith(input);
    row.classList.add("is-renaming");

    let finished = false;
    const cleanup = (nextTitle) => {
      if (finished) return;
      finished = true;
      row.classList.remove("is-renaming");
      const span = document.createElement("span");
      span.className = "nav-label";
      span.textContent = nextTitle;
      input.replaceWith(span);
    };

    const cancel = () => cleanup(originalTitle);
    const commit = async () => {
      if (finished) return;
      const normalized = String(input.value || "").trim();
      if (!normalized || normalized === originalTitle) {
        cancel();
        return;
      }
      try {
        const updated = await renameConversation(renameUrl, normalized);
        cleanup(String(updated?.title || normalized).trim() || originalTitle);
      } catch (error) {
        console.error(error);
        cancel();
      }
    };

    input.addEventListener("mousedown", (event) => event.stopPropagation());
    input.addEventListener("click", (event) => event.stopPropagation());
    input.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        event.stopPropagation();
        void commit();
        return;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        event.stopPropagation();
        cancel();
      }
    });
    input.addEventListener("blur", () => void commit(), { once: true });
    requestAnimationFrame(() => {
      input.focus();
      input.select();
    });
  };

  conversationListEl?.addEventListener("click", async (event) => {
    const menuItem = event.target?.closest?.(".chat-nav-menu-item");
    const menuAction = menuItem?.dataset?.action;
    const conversationId = menuItem?.dataset?.conversationId;
    const renameUrl = menuItem?.dataset?.renameUrl;
    const deleteUrl = menuItem?.dataset?.deleteUrl;
    const currentConversationId = menuItem?.dataset?.currentConversationId;
    if (menuAction === "rename" && conversationId && renameUrl) {
      event.preventDefault();
      event.stopPropagation();
      closeConversationMenu();
      beginRenameConversation(conversationId, menuItem.closest(".chat-nav-row"), renameUrl);
      return;
    }
    if (menuAction === "delete" && conversationId && deleteUrl) {
      event.preventDefault();
      event.stopPropagation();
      closeConversationMenu();
      const confirmed = await showEcosystemConfirm({
        title: "Delete chat?",
        message: "Remove this conversation from CreatureOS? This cannot be undone.",
        confirmLabel: "Delete chat",
        variant: "danger",
      });
      if (!confirmed) {
        return;
      }
      void deleteConversation(deleteUrl, currentConversationId)
        .then((payload) => {
          const redirectUrl = String(payload?.redirect_url || currentConversationUrl());
          window.location.assign(redirectUrl);
        })
        .catch((error) => {
          console.error(error);
        });
      return;
    }

    const menuButton = event.target?.closest?.(".chat-nav-menu");
    const menuId = menuButton?.dataset?.conversationId;
    if (menuId) {
      event.preventDefault();
      event.stopPropagation();
      toggleConversationMenu(menuId);
    }
  });

  document.addEventListener("click", (event) => {
    if (!conversationListEl) return;
    const target = event.target;
    const insideMenu = target?.closest?.(".chat-nav-menu-popover");
    const insideButton = target?.closest?.(".chat-nav-menu");
    const insideRename = target?.closest?.(".chat-rename-input");
    if (insideMenu || insideButton || insideRename) return;
    closeConversationMenu();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape" || !openConversationMenuId) return;
    event.preventDefault();
    closeConversationMenu({ focus: true });
  });

  const scrollChatToBottom = () => {
    scrollChatViewportToBottom();
    scheduleChatViewportAutoScroll();
  };

  let activeRunFeedSource = null;

  const closeRunFeedStream = () => {
    if (!activeRunFeedSource) return;
    activeRunFeedSource.close();
    activeRunFeedSource = null;
  };

  const formatBytesCompact = (value) => {
    const numeric = Number(value || 0);
    if (!Number.isFinite(numeric) || numeric <= 0) return "";
    const units = ["B", "KB", "MB", "GB"];
    let size = numeric;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex += 1;
    }
    if (unitIndex === 0) {
      return `${Math.round(size)} ${units[unitIndex]}`;
    }
    const rounded = Math.round(size * 10) / 10;
    return `${String(rounded).replace(/\\.0$/, "")} ${units[unitIndex]}`;
  };

  const selectedUploadFiles = () => {
    if (!(uploadInput instanceof HTMLInputElement) || !uploadInput.files) return [];
    return Array.from(uploadInput.files);
  };

  const setUploadFiles = (files) => {
    if (!(uploadInput instanceof HTMLInputElement)) return;
    const transfer = new DataTransfer();
    for (const file of files) {
      if (file instanceof File) {
        transfer.items.add(file);
      }
    }
    uploadInput.files = transfer.files;
  };

  const mergeUploadFiles = (incomingFiles) => {
    const merged = [];
    const seen = new Set();
    for (const file of [...selectedUploadFiles(), ...incomingFiles]) {
      if (!(file instanceof File)) continue;
      const key = `${file.name}::${file.size}::${file.lastModified}::${file.type}`;
      if (seen.has(key)) continue;
      seen.add(key);
      merged.push(file);
    }
    setUploadFiles(merged);
  };

  const buildAttachmentPreviewNode = ({ name = "", sizeLabel = "", type = "", url = "" } = {}) => {
    const link = document.createElement(url ? "a" : "div");
    link.className = type.startsWith("image/") && url
      ? "creature-message-attachment creature-message-attachment--image"
      : "creature-message-attachment";
    if (url && link instanceof HTMLAnchorElement) {
      link.href = url;
      link.target = "_blank";
      link.rel = "noopener";
    }
    if (type.startsWith("image/") && url) {
      const image = document.createElement("img");
      image.src = url;
      image.alt = name;
      image.loading = "lazy";
      link.appendChild(image);
      const label = document.createElement("span");
      label.textContent = name;
      link.appendChild(label);
      return link;
    }
    const strong = document.createElement("strong");
    strong.textContent = name;
    const meta = document.createElement("span");
    meta.textContent = sizeLabel;
    link.append(strong, meta);
    return link;
  };

  const renderComposeAttachments = () => {
    if (!(composeAttachmentsEl instanceof HTMLElement)) return;
    const files = selectedUploadFiles();
    composeAttachmentsEl.textContent = "";
    composeAttachmentsEl.hidden = files.length === 0;
    files.forEach((file, index) => {
      const chip = document.createElement("div");
      chip.className = "creature-compose-attachment-chip";
      const summary = document.createElement("div");
      summary.className = "creature-compose-attachment-summary";
      const name = document.createElement("strong");
      name.textContent = file.name;
      const meta = document.createElement("span");
      meta.textContent = formatBytesCompact(file.size);
      summary.append(name, meta);
      const removeButton = document.createElement("button");
      removeButton.type = "button";
      removeButton.className = "creature-compose-attachment-remove";
      removeButton.textContent = "Remove";
      removeButton.dataset.attachmentIndex = String(index);
      chip.append(summary, removeButton);
      composeAttachmentsEl.appendChild(chip);
    });
  };

  const setChatThinkingNote = (text, state = "") => {
    if (!(chatThinkingNote instanceof HTMLElement)) return;
    const nextText = String(text || "").trim();
    chatThinkingNote.textContent = nextText;
    chatThinkingNote.dataset.state = state;
    chatThinkingNote.hidden = nextText.length === 0;
  };

  const ensureRunFeedPanel = ({ runId = "", status = "", streamUrl = "" } = {}) => {
    const composer = form.closest(".composer");
    const shell = form.closest(".creature-compose-shell");
    if (!(composer instanceof HTMLElement) || !(shell instanceof HTMLElement)) return null;

    let panel = document.getElementById("creature-run-feed");
    if (!(panel instanceof HTMLDetailsElement)) {
      panel = document.createElement("details");
      panel.id = "creature-run-feed";
      panel.className = "creature-run-feed creature-run-feed--composer";
      panel.dataset.runFeed = "true";

      const summary = document.createElement("summary");
      const title = document.createElement("span");
      title.className = "creature-compose-subtle-toggle-label";
      title.dataset.runFeedLabel = "";
      title.textContent = runFeedLabelForStatus(status);
      const statusEl = document.createElement("span");
      statusEl.className = "creature-run-feed-status";
      summary.append(title, statusEl);

      const output = document.createElement("pre");
      output.id = "creature-run-feed-output";
      output.className = "creature-run-feed-output";
      output.textContent = "Waiting for first output…";
      output.dataset.waitingPlaceholder = "true";

      panel.append(summary, output);
      composer.insertBefore(panel, shell);
    } else if (panel.parentElement !== composer) {
      composer.insertBefore(panel, shell);
    }

    const output = panel.querySelector("#creature-run-feed-output");
    const existingRunId = String(panel.dataset.runId || "");
    if (runId && existingRunId && existingRunId !== String(runId) && output instanceof HTMLElement) {
      output.textContent = "";
      output.dataset.waitingPlaceholder = "true";
      liveThinkingFeedState.delete(output);
      liveThinkingFeedOutputs.delete(output);
      panel.open = false;
    }

    if (runId) panel.dataset.runId = String(runId);
    if (status) panel.dataset.runStatus = String(status);
    if (streamUrl) panel.dataset.streamUrl = String(streamUrl);
    panel.hidden = false;
    syncRunFeedExpandedState(panel, String(status || panel.dataset.runStatus || ""));

    const statusEl = panel.querySelector(".creature-run-feed-status");
    setRunFeedSummaryLabel(panel, String(status || panel.dataset.runStatus || ""));
    setFeedStatusState(statusEl, String(status || panel.dataset.runStatus || ""));
    return panel;
  };

  const replaceRunFeedLines = ({ runId = "", status = "", streamUrl = "", lines = [] } = {}) => {
    const panel = ensureRunFeedPanel({ runId, status, streamUrl });
    if (!(panel instanceof HTMLElement)) return null;
    const output = panel.querySelector("#creature-run-feed-output");
    if (!(output instanceof HTMLElement)) return panel;
    const nextLines = Array.isArray(lines)
      ? lines.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    updateLiveThinkingFeed(output, {
      lines: nextLines,
      status: String(status || panel.dataset.runStatus || "idle"),
      placeholder: "Waiting for first output…",
    });
    return panel;
  };

  const hideRunFeedPanel = () => {
    const panel = document.getElementById("creature-run-feed");
    if (!(panel instanceof HTMLDetailsElement)) return;
    panel.dataset.runStatus = "idle";
    delete panel.dataset.runId;
    delete panel.dataset.streamUrl;
    delete panel.dataset.lastEventId;
    syncRunFeedExpandedState(panel, "idle");
    panel.hidden = true;
    const statusEl = panel.querySelector(".creature-run-feed-status");
    setRunFeedSummaryLabel(panel, "idle");
    setFeedStatusState(statusEl, "idle");
    const output = panel.querySelector("#creature-run-feed-output");
    if (output instanceof HTMLElement) {
      updateLiveThinkingFeed(output, {
        lines: [],
        status: "idle",
        placeholder: "Waiting for first output…",
      });
    }
  };

  const appendRunFeedLine = (line) => {
    const text = String(line || "").trim();
    if (!text) return;
    const panel = ensureRunFeedPanel();
    if (!(panel instanceof HTMLElement)) return;
    const output = panel.querySelector("#creature-run-feed-output");
    if (!(output instanceof HTMLElement)) return;
    const previousState = liveThinkingFeedState.get(output);
    const nextLines = Array.isArray(previousState?.lines)
      ? [...previousState.lines, text]
      : (output.textContent || "")
          .split("\n")
          .map((item) => String(item || "").trim())
          .filter(Boolean)
          .concat(text);
    updateLiveThinkingFeed(output, {
      lines: nextLines,
      status: String(panel.dataset.runStatus || "running"),
      placeholder: "Waiting for first output…",
    });
  };

  const startRunFeedStream = ({ runId = "", streamUrl = "", redirectUrl = "", lastEventId = 0, showPanel = true } = {}) => {
    if (!streamUrl) return;
    closeRunFeedStream();
    if (showPanel) {
      ensureRunFeedPanel({ runId, status: "running", streamUrl });
    }
    const streamTarget = new URL(streamUrl, window.location.origin);
    if (Number.isFinite(Number(lastEventId)) && Number(lastEventId) > 0) {
      streamTarget.searchParams.set("last_event_id", String(lastEventId));
    }
    const source = new EventSource(streamTarget.toString());
    activeRunFeedSource = source;
    source.onmessage = (event) => {
      let payload = {};
      try {
        payload = JSON.parse(String(event.data || "{}"));
      } catch {}
      if (showPanel) {
        appendRunFeedLine(payload?.display_body || payload?.body || "");
      }
      const nextStatus = String(payload?.run_status || "").trim();
      if (!nextStatus || nextStatus === "running") return;
      if (showPanel) {
        const panel = ensureRunFeedPanel({ runId, status: nextStatus, streamUrl });
        const output = panel?.querySelector?.("#creature-run-feed-output");
        if (output instanceof HTMLElement) {
          const previousState = liveThinkingFeedState.get(output);
          updateLiveThinkingFeed(output, {
            lines: Array.isArray(previousState?.lines) ? previousState.lines : [],
            status: nextStatus,
            placeholder: "Waiting for first output…",
          });
        }
      }
      closeRunFeedStream();
      window.location.assign(redirectUrl || currentConversationUrl());
    };
  };

  const syncInputHeight = () => {
    const composerHeight = Number.parseInt(getComputedStyle(root).getPropertyValue("--composer-h"), 10);
    const minInputHeight = Number.isFinite(composerHeight) && composerHeight > 0 ? composerHeight : 38;
    input.style.height = "auto";
    const nextHeight = Math.min(Math.max(input.scrollHeight, minInputHeight), 220);
    input.style.height = `${nextHeight}px`;
    input.style.overflowY = input.scrollHeight > 220 ? "auto" : "hidden";
    root.dataset.chatInputExpanded = nextHeight > (minInputHeight + 8) ? "true" : "false";
  };

  const appendMessageRow = (role, contentNode) => {
    const row = document.createElement("div");
    row.className = `row ${role}`;
    const bubble = document.createElement("div");
    const isKeeperComposer = form.matches(".keeper-compose-form");
    bubble.className = role === "assistant" && isKeeperComposer ? "bubble bubble--full-span" : "bubble";
    bubble.appendChild(contentNode);
    row.appendChild(bubble);
    rail.appendChild(row);
    scrollChatToBottom();
    return row;
  };

  const appendUserMessage = (text, attachments = []) => {
    const content = document.createElement("div");
    if (text) {
      const body = document.createElement("div");
      body.className = "creature-bubble-text";
      body.textContent = text;
      content.appendChild(body);
    }
    if (attachments.length) {
      const attachmentWrap = document.createElement("div");
      attachmentWrap.className = "creature-message-attachments";
      attachments.forEach((item) => {
        attachmentWrap.appendChild(buildAttachmentPreviewNode(item));
      });
      content.appendChild(attachmentWrap);
    }
    return appendMessageRow("user", content);
  };

  const appendAssistantMarkdownMessage = (markdown) => {
    const content = document.createElement("div");
    const isKeeperComposer = form.matches(".keeper-compose-form");
    if (markdown) {
      const source = document.createElement("script");
      source.type = "text/plain";
      source.className = "creature-markdown-source";
      source.setAttribute("data-markdown-source", "");
      source.textContent = String(markdown);
      const target = document.createElement("div");
      target.className = isKeeperComposer
        ? "creature-message-content creature-markdown-content--pending keeper-dialog-body"
        : "creature-message-content creature-markdown-content--pending";
      target.setAttribute("data-markdown-target", "");
      if (isKeeperComposer) {
        target.dataset.typewriterText = String(markdown || "");
        target.dataset.typewriterSpeed = "12";
        target.dataset.typewriterMarkdown = "true";
      }
      content.append(source, target);
      if (!isKeeperComposer) {
        renderMarkdownIntoTarget(target);
      }
    }
    const row = appendMessageRow("assistant", content);
    if (isKeeperComposer) {
      const target = content.querySelector("[data-markdown-target]");
      if (target instanceof HTMLElement && !startTypewriterOnNode(target)) {
        renderMarkdownIntoTarget(target);
      }
    }
    return row;
  };

  const ensureConversationIdField = (conversationId) => {
    const cid = Number.parseInt(String(conversationId || ""), 10);
    if (!Number.isFinite(cid) || cid <= 0) return;
    let field = form.querySelector('input[name="conversation_id"]');
    if (!(field instanceof HTMLInputElement)) {
      field = document.createElement("input");
      field.type = "hidden";
      field.name = "conversation_id";
      form.prepend(field);
    }
    field.value = String(cid);
  };

  const setSubmitting = (isSubmitting) => {
    input.disabled = isSubmitting;
    if (uploadButton instanceof HTMLButtonElement) {
      uploadButton.disabled = isSubmitting;
    }
    submitButtons.forEach((buttonEl) => {
      buttonEl.disabled = isSubmitting;
    });
    form.dataset.submitting = isSubmitting ? "true" : "false";
  };

  syncInputHeight();
  input.addEventListener("input", syncInputHeight);
  renderComposeAttachments();

  if (uploadButton instanceof HTMLButtonElement && uploadInput instanceof HTMLInputElement) {
    uploadButton.addEventListener("click", () => uploadInput.click());
    uploadInput.addEventListener("change", renderComposeAttachments);
  }

  composeAttachmentsEl?.addEventListener("click", (event) => {
    const removeButton = event.target?.closest?.("[data-attachment-index]");
    if (!(removeButton instanceof HTMLButtonElement)) return;
    const index = Number.parseInt(String(removeButton.dataset.attachmentIndex || ""), 10);
    if (!Number.isFinite(index)) return;
    const remainingFiles = selectedUploadFiles().filter((_, itemIndex) => itemIndex !== index);
    setUploadFiles(remainingFiles);
    renderComposeAttachments();
  });

  input.addEventListener("paste", (event) => {
    const items = Array.from(event.clipboardData?.items || []);
    const pastedFiles = items
      .map((item) => (item.kind === "file" ? item.getAsFile() : null))
      .filter((file) => file instanceof File);
    if (!pastedFiles.length) return;
    event.preventDefault();
    mergeUploadFiles(pastedFiles);
    renderComposeAttachments();
  });

  if (chatThinkingForm instanceof HTMLFormElement) {
    let thinkingSaveTimer = 0;
    let thinkingSubmitting = false;
    const submitChatThinkingForm = async () => {
      if (thinkingSubmitting) return;
      thinkingSubmitting = true;
      setChatThinkingNote("Saving…", "saving");
      try {
        const response = await fetch(chatThinkingForm.action, {
          method: "POST",
          headers: { "x-creatureos-ajax": "1" },
          body: new FormData(chatThinkingForm),
        });
        if (!response.ok) {
          throw new Error(`Thinking save failed with status ${response.status}`);
        }
        const payload = await response.json();
        const model = String(payload?.model || "").trim();
        const effort = String(payload?.reasoning_effort_label || payload?.reasoning_effort || "").trim();
        setChatThinkingNote(`Saved · ${model}${effort ? ` · ${effort}` : ""}`, "saved");
        window.clearTimeout(thinkingSaveTimer);
        thinkingSaveTimer = window.setTimeout(() => {
          setChatThinkingNote("", "");
        }, 1800);
      } catch (error) {
        console.error(error);
        setChatThinkingNote("Could not save right now.", "error");
      } finally {
        thinkingSubmitting = false;
      }
    };

    chatThinkingForm.querySelectorAll("[data-chat-thinking-field]").forEach((fieldEl) => {
      fieldEl.addEventListener("change", () => {
        void submitChatThinkingForm();
      });
    });
  }

  input.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" || event.shiftKey) return;
    event.preventDefault();
    if (!String(input.value || "").trim() && selectedUploadFiles().length === 0) return;
    const submitTarget = defaultBusySubmit instanceof HTMLButtonElement
      ? defaultBusySubmit
      : (sendButton instanceof HTMLButtonElement ? sendButton : submitButtons[0] || null);
    if (submitTarget instanceof HTMLButtonElement) {
      form.requestSubmit(submitTarget);
      return;
    }
    form.requestSubmit();
  });

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (form.dataset.submitting === "true") return;

    const message = String(input.value || "").trim();
    const pendingFiles = selectedUploadFiles();
    if (!message && pendingFiles.length === 0) return;

    const formData = new FormData(form);
    const submitter = event.submitter instanceof HTMLButtonElement
      ? event.submitter
      : (defaultBusySubmit instanceof HTMLButtonElement ? defaultBusySubmit : (sendButton instanceof HTMLButtonElement ? sendButton : null));
    if (submitter instanceof HTMLButtonElement && submitter.name) {
      formData.set(submitter.name, submitter.value);
    }
    appendUserMessage(
      message,
      pendingFiles.map((file) => ({
        name: file.name,
        sizeLabel: formatBytesCompact(file.size),
        type: String(file.type || ""),
        url: String(file.type || "").startsWith("image/") ? URL.createObjectURL(file) : "",
      })),
    );

    input.value = "";
    setUploadFiles([]);
    renderComposeAttachments();
    syncInputHeight();
    setSubmitting(true);
    closeRunFeedStream();
    replaceRunFeedLines({ status: "running", lines: [] });

    try {
      const response = await fetch(form.action, {
        method: "POST",
        body: formData,
        headers: { "x-creatureos-ajax": "1" },
      });
      if (!response.ok) {
        const failurePayload = await response.json().catch(() => ({}));
        throw new Error(String(failurePayload?.detail || `Request failed with status ${response.status}`));
      }
      const payload = await response.json();
      const redirectUrl = String(payload?.redirect_url || "");
      const streamUrl = String(payload?.stream_url || "");
      const runId = String(payload?.run_id || "");
      const status = String(payload?.status || "running");
      const busyAction = String(payload?.busy_action || "queue");
      const runScope = String(payload?.run_scope || "");
      const waitingMessage = String(payload?.waiting_message || "").trim();
      if (status === "waiting") {
        if (redirectUrl && redirectUrl !== currentConversationUrl()) {
          window.history.replaceState({}, "", redirectUrl);
          ensureConversationIdField(payload?.conversation_id);
        }
        hideRunFeedPanel();
        const content = document.createElement("div");
        content.className = "creature-bubble-text";
        content.textContent = waitingMessage || "CreatureOS is waiting for Codex right now.";
        appendMessageRow("system", content);
        setSubmitting(false);
        input.focus();
        return;
      }
      if (!redirectUrl || !streamUrl || !runId) {
        throw new Error("Missing run stream payload");
      }
      if (status === "locked") {
        ensureRunFeedPanel({ runId, status: "running", streamUrl });
        appendRunFeedLine(
          busyAction === "steer"
            ? "Will steer after current work…"
            : "Queued behind current work…",
        );
      }
      startRunFeedStream({
        runId,
        streamUrl,
        redirectUrl,
        showPanel: true,
      });
    } catch (error) {
      closeRunFeedStream();
      hideRunFeedPanel();
      const content = document.createElement("div");
      content.className = "creature-bubble-text";
      const detail = String(error instanceof Error ? error.message : error || "").trim();
      content.textContent = detail ? `Send failed. ${detail}` : "Send failed. Refresh and retry if the reply does not appear.";
      appendMessageRow("system", content);
      input.value = message;
      setUploadFiles(pendingFiles);
      renderComposeAttachments();
      syncInputHeight();
      setSubmitting(false);
      input.focus();
      console.error(error);
    }
  });

  const existingRunFeed = document.getElementById("creature-run-feed");
  if (existingRunFeed instanceof HTMLElement) {
    const existingStatus = String(existingRunFeed.dataset.runStatus || "").trim();
    const existingStreamUrl = String(existingRunFeed.dataset.streamUrl || "").trim();
    const existingRunId = String(existingRunFeed.dataset.runId || "").trim();
    const existingLastEventId = Number.parseInt(String(existingRunFeed.dataset.lastEventId || "0"), 10) || 0;
    const existingOutput = existingRunFeed.querySelector("#creature-run-feed-output");
    if (existingOutput instanceof HTMLElement) {
      const initialLines = (existingOutput.textContent || "")
        .split("\n")
        .map((item) => String(item || "").trim())
        .filter(Boolean)
        .filter((item) => item !== "Waiting for first output…");
      updateLiveThinkingFeed(existingOutput, {
        lines: initialLines,
        status: existingStatus || "idle",
        placeholder: "Waiting for first output…",
      });
    }
    if (existingStatus === "running" && existingStreamUrl) {
      startRunFeedStream({
        runId: existingRunId,
        streamUrl: existingStreamUrl,
        redirectUrl: currentConversationUrl(),
        lastEventId: existingLastEventId,
      });
    }
  }
})();
