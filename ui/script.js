const { createApp, ref, reactive, nextTick, onMounted, watch, onBeforeUnmount, triggerRef } = Vue;
const { ElMessage } = ElementPlus;

const app = createApp({
    setup() {
        // 状态定义
        const messages = ref([]);
        const inputMessage = ref('');
        const isLoading = ref(false);
        const chatWrapper = ref(null);
        const createSessionId = () => 'session_' + Date.now();
        let sessionId = createSessionId();
        const currentThreadTitle = ref('');
        const enableSuggestions = ref(false); // 默认关闭开关
        const isDarkTheme = ref(true);
        const STORAGE_KEY = 'text2sql_guest_chat_state_v2';
        const LEGACY_STORAGE_KEY = 'text2sql_chat_state_v1';
        const AUTH_STORAGE_KEY = 'text2sql_auth_state_v1';
        const AUTH_TOKEN_KEY = 'text2sql_auth_token_v1';
        const AUTH_USER_KEY = 'text2sql_auth_user_v1';
        const APP_MODE_KEY = 'text2sql_app_mode_v1';
        const LOCAL_DRAFT_KEY = 'text2sql_draft_v1';
        const MAX_PERSIST_MESSAGES = 80;
        const MAX_THREAD_COUNT = 50;

        // 当前视图状态
        const currentView = ref('chat'); // 'chat' | 'modeling' | 'database'

        // 建模页面数据
        const metricsData = ref(null);
        const metricsLoading = ref(false);
        const metricsExpanded = reactive({});

        // 数据库页面数据
        const schemaData = ref(null);
        const schemaLoading = ref(false);

        // 计划审核相关
        const planReviewVisible = ref(false);
        const planReviewData = ref(null);
        const planReviewAdjustments = ref('');

        // 当前回答的步骤（用于流式显示）
        const currentSteps = ref([]);

        const authToken = ref('');
        const currentUser = ref(null);

        // 数据展示弹窗
        const dialogVisible = ref(false);
        const tableData = ref([]);
        const nestedTableSections = ref([]);

        // SQL 展示弹窗
        const sqlDialogVisible = ref(false);
        const currentSql = ref('');

        // 图表展示弹窗
        const chartDialogVisible = ref(false);
        const currentChartSpec = ref(null);
        const currentChartReasoning = ref('');

        // 打字机效果处理函数
        const typeWriter = (obj, key, text, speed = 15) => {
            if (!text) return;
            // 如果已经在输入相同内容，跳过
            if (obj[key] && text.startsWith(obj[key]) && obj[key].length > 0 && obj[key].length < text.length) {
                // 部分更新逻辑（如果需要支持真正的流式，但目前节点输出是完整的）
            }

            let i = 0;
            obj[key] = "";
            const timer = setInterval(() => {
                if (i < text.length) {
                    obj[key] += text.charAt(i);
                    i++;
                } else {
                    clearInterval(timer);
                }
            }, speed);
        };


        const normalizeTableData = (data) => {
            if (Array.isArray(data)) return data;
            if (data && typeof data === 'object') return [data];
            if (data === null || data === undefined) return [];
            return [{ value: data }];
        };

        const isRecordArray = (value) => {
            if (!Array.isArray(value) || value.length === 0) return false;
            return value.every((item) => item && typeof item === 'object' && !Array.isArray(item));
        };

        const prepareDialogTableData = (rows) => {
            const sections = [];
            const displayRows = rows.map((row) => {
                if (!row || typeof row !== 'object' || Array.isArray(row)) {
                    return row;
                }

                const nextRow = { ...row };
                Object.keys(nextRow).forEach((key) => {
                    const value = nextRow[key];
                    if (isRecordArray(value)) {
                        sections.push({
                            key,
                            title: `${key} 明细`,
                            data: value
                        });
                        nextRow[key] = `见下方明细表（${value.length} 条）`;
                    }
                });
                return nextRow;
            });

            return { displayRows, sections };
        };

        const hasResultData = (msg) => {
            const data = msg?.sqlResult;
            if (Array.isArray(data)) return data.length > 0;
            if (data && typeof data === 'object') return Object.keys(data).length > 0;
            return data !== null && data !== undefined && data !== '';
        };

        const hasReplaySource = (msg) => {
            return Boolean((msg?.sql && String(msg.sql).trim()) || (msg?.pythonCode && String(msg.pythonCode).trim()));
        };

        const canViewData = (msg) => {
            return hasResultData(msg) || hasReplaySource(msg);
        };

        const canGenerateChart = (msg) => {
            const data = msg?.sqlResult;
            return Array.isArray(data) && data.length > 0;
        };

        const fetchReplayData = async (msg) => {
            const response = await fetch('/api/replay-data', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    session_id: sessionId,
                    sql: msg?.sql || null,
                    python_code: msg?.pythonCode || null
                })
            });

            if (!response.ok) {
                throw new Error('回放数据请求失败');
            }

            const payload = await response.json();
            if (payload.error) {
                throw new Error(payload.error);
            }

            return payload.data;
        };

        const showData = async (msgOrData) => {
            let data = msgOrData;
            const isMessageObject = msgOrData && typeof msgOrData === 'object' && !Array.isArray(msgOrData) && 'role' in msgOrData;

            if (isMessageObject) {
                const msg = msgOrData;
                // 如果数据被截断，先尝试回放获取完整数据
                if (msg.isTruncated) {
                    msg.isReplayLoading = true;
                    try {
                        const replayData = await fetchReplayData(msg);
                        msg.sqlResult = replayData;
                        msg.isTruncated = false; // 标记为已获取完整数据
                        data = replayData;
                        ElMessage.success(`已加载全部 ${msg.totalCount || replayData.length} 条数据`);
                    } catch (e) {
                        ElMessage.error(`加载完整数据失败: ${e.message || e}，显示前100条`);
                        data = msg.sqlResult; // 回退到已有数据
                    } finally {
                        msg.isReplayLoading = false;
                    }
                } else if (!hasResultData(msg) && hasReplaySource(msg)) {
                    msg.isReplayLoading = true;
                    try {
                        const replayData = await fetchReplayData(msg);
                        msg.sqlResult = replayData;
                        data = replayData;
                        ElMessage.success('已回放并加载数据');
                    } catch (e) {
                        ElMessage.error(`回放失败: ${e.message || e}`);
                        return;
                    } finally {
                        msg.isReplayLoading = false;
                    }
                } else {
                    data = msg.sqlResult;
                }
            }

            const normalizedRows = normalizeTableData(data);
            const prepared = prepareDialogTableData(normalizedRows);
            tableData.value = prepared.displayRows;
            nestedTableSections.value = prepared.sections;
            dialogVisible.value = true;
        };

        const showSql = (sql) => {
            currentSql.value = sql;
            sqlDialogVisible.value = true;
        };

        const viewChart = (spec, reasoning) => {
            currentChartSpec.value = spec;
            currentChartReasoning.value = reasoning;
            chartDialogVisible.value = true;

            // 等待 Dialog DOM 渲染完成后嵌入图表
            const tryEmbed = (retries = 0) => {
                const container = document.querySelector('#dialog-chart-container');
                if (container) {
                    try {
                        const rawSpec = JSON.parse(JSON.stringify(spec));
                        rawSpec.width = "container";
                        rawSpec.height = "container";
                        rawSpec.autosize = { type: "fit", contains: "padding" };
                        vegaEmbed('#dialog-chart-container', rawSpec, {
                            actions: false,
                            renderer: 'svg'
                        }).catch(e => {
                            console.error('Vega Embed Error:', e);
                            ElMessage.error('图表渲染出错');
                        });
                    } catch (e) {
                        console.error('Chart view error:', e);
                    }
                } else if (retries < 20) {
                    setTimeout(() => tryEmbed(retries + 1), 50);
                } else {
                    console.error('Chart container not found after retries');
                    ElMessage.error('图表容器未就绪');
                }
            };
            nextTick(() => setTimeout(() => tryEmbed(), 100));
        };

        // 推荐问题（动态获取）
        const suggestedQuestions = ref([]);
        const threadList = ref([]);
        const activeThreadId = ref('');
        let suspendPersist = false;

        // 侧边栏拖拽调整宽度
        const sidebarWidth = ref(280);
        let isResizingSidebar = false;
        let resizeStartX = 0;
        let resizeStartWidth = 0;

        const startResizeSidebar = (e) => {
            isResizingSidebar = true;
            resizeStartX = e.clientX;
            resizeStartWidth = sidebarWidth.value;
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            document.addEventListener('mousemove', onResizeSidebar);
            document.addEventListener('mouseup', stopResizeSidebar);
        };

        const onResizeSidebar = (e) => {
            if (!isResizingSidebar) return;
            const delta = e.clientX - resizeStartX;
            const newWidth = Math.max(200, Math.min(400, resizeStartWidth + delta));
            sidebarWidth.value = newWidth;
            document.documentElement.style.setProperty('--sidebar-width', `${newWidth}px`);
        };

        const stopResizeSidebar = () => {
            isResizingSidebar = false;
            document.body.style.cursor = '';
            document.body.style.userSelect = '';
            document.removeEventListener('mousemove', onResizeSidebar);
            document.removeEventListener('mouseup', stopResizeSidebar);
        };

        const createThreadId = () => `thread_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
        const isAuthenticated = () => Boolean(authToken.value);
        const getActiveDraftKey = () => `${LOCAL_DRAFT_KEY}:${activeThreadId.value || 'default'}`;
        const createHeaders = (extraHeaders = {}) => {
            const headers = { ...extraHeaders };
            if (isAuthenticated()) {
                headers.Authorization = `Bearer ${authToken.value}`;
            }
            return headers;
        };
        const parseServerTime = (value) => {
            if (!value) return Date.now();
            const ts = Date.parse(value);
            return Number.isNaN(ts) ? Date.now() : ts;
        };
        const inflateMessageForRuntime = (msg) => ({
            ...msg,
            steps: Array.isArray(msg?.steps) ? msg.steps : [],
            clarificationSections: Array.isArray(msg?.clarificationSections) ? msg.clarificationSections : [],
            isStreaming: false,
            isChartLoading: false,
            isReplayLoading: false,
            metricPlan: msg?.metricPlan || null,
            metricStepStatuses: msg?.metricStepStatuses || {},
            metricStepSqls: msg?.metricStepSqls || {},
            metricStepResults: msg?.metricStepResults || {},
            _expandedSqls: msg?._expandedSqls || {}
        });
        const normalizeConversationSummary = (thread = {}) => ({
            id: thread.id || createThreadId(),
            sessionId: thread.session_id || thread.sessionId || createSessionId(),
            workspaceId: thread.workspace_id || thread.workspaceId || '',
            title: thread.title || '',
            messages: Array.isArray(thread.messages) ? thread.messages.map((msg) => inflateMessageForRuntime(msg)) : [],
            suggestedQuestions: Array.isArray(thread.suggested_questions) ? thread.suggested_questions : (Array.isArray(thread.suggestedQuestions) ? thread.suggestedQuestions : []),
            enableSuggestions: Boolean(
                typeof thread.enable_suggestions === 'boolean' ? thread.enable_suggestions : thread.enableSuggestions
            ),
            lastMessagePreview: thread.last_message_preview || thread.lastMessagePreview || '',
            updatedAt: parseServerTime(thread.updated_at || thread.updatedAt),
            createdAt: parseServerTime(thread.created_at || thread.createdAt)
        });
        const saveDraft = (value) => {
            try {
                localStorage.setItem(getActiveDraftKey(), value || '');
            } catch (e) {
                console.warn('保存草稿失败:', e);
            }
        };
        const restoreDraft = () => {
            try {
                inputMessage.value = localStorage.getItem(getActiveDraftKey()) || '';
            } catch (e) {
                console.warn('恢复草稿失败:', e);
            }
        };
        const clearDraft = () => {
            try {
                localStorage.removeItem(getActiveDraftKey());
            } catch (e) {
                console.warn('清理草稿失败:', e);
            }
        };
        const persistAuthState = () => {
            try {
                const payload = {
                    version: 1,
                    activeThreadId: activeThreadId.value,
                    uiTheme: isDarkTheme.value ? 'dark' : 'light'
                };
                localStorage.setItem(AUTH_STORAGE_KEY, JSON.stringify(payload));
            } catch (e) {
                console.warn('保存鉴权状态失败:', e);
            }
            saveDraft(inputMessage.value);
        };
        const clearAuthState = () => {
            try {
                localStorage.removeItem(AUTH_STORAGE_KEY);
                localStorage.removeItem(AUTH_TOKEN_KEY);
                localStorage.removeItem(AUTH_USER_KEY);
            } catch (e) {
                console.warn('清理鉴权状态失败:', e);
            }
        };
        const setAppMode = (mode) => {
            try {
                if (mode) {
                    localStorage.setItem(APP_MODE_KEY, mode);
                } else {
                    localStorage.removeItem(APP_MODE_KEY);
                }
            } catch (e) {
                console.warn('设置应用模式失败:', e);
            }
        };
        const applyThreadSummaryUpdate = (thread) => {
            const normalized = normalizeConversationSummary(thread);
            const idx = threadList.value.findIndex((item) => item.id === normalized.id);
            if (idx >= 0) {
                threadList.value[idx] = {
                    ...threadList.value[idx],
                    ...normalized
                };
            } else {
                threadList.value.unshift(normalized);
            }
            threadList.value = threadList.value
                .sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0))
                .slice(0, MAX_THREAD_COUNT);
        };
        const setAuthenticatedUser = (token, user) => {
            authToken.value = token || '';
            currentUser.value = user || null;
            if (token) {
                localStorage.setItem(AUTH_TOKEN_KEY, token);
            } else {
                localStorage.removeItem(AUTH_TOKEN_KEY);
            }
            if (user) {
                localStorage.setItem(AUTH_USER_KEY, JSON.stringify(user));
            } else {
                localStorage.removeItem(AUTH_USER_KEY);
            }
        };
        const handleAuthFailure = (error) => {
            console.warn('鉴权请求失败，回退到本地模式:', error);
            clearAuthState();
            authToken.value = '';
            currentUser.value = null;
            ElMessage.warning('登录态已失效，请重新登录');
        };
        const apiFetch = async (url, options = {}) => {
            const response = await fetch(url, {
                ...options,
                headers: createHeaders(options.headers || {})
            });
            if (response.status === 401) {
                handleAuthFailure('unauthorized');
                throw new Error('登录已过期，请重新登录');
            }
            return response;
        };

        const saveAuthenticatedHistory = async (conversationId, messageBatch) => {
            if (!isAuthenticated() || !conversationId || !Array.isArray(messageBatch) || messageBatch.length === 0) {
                return;
            }

            const response = await apiFetch(`/api/conversations/${conversationId}/messages`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    session_id: sessionId,
                    title: currentThreadTitle.value || '',
                    enable_suggestions: enableSuggestions.value,
                    suggested_questions: suggestedQuestions.value,
                    messages: messageBatch.map((msg) => serializeMessageForStorage(msg))
                })
            });
            if (!response.ok) {
                const payload = await response.json().catch(() => ({}));
                throw new Error(payload.detail || '保存历史失败');
            }
            const payload = await response.json();
            const summary = normalizeConversationSummary({
                ...payload.conversation,
                messages: Array.isArray(payload.messages) ? payload.messages : []
            });
            applyThreadSummaryUpdate(summary);
        };

        const applyTheme = (darkMode) => {
            const theme = darkMode ? 'dark' : 'light';
            document.documentElement.setAttribute('data-theme', theme);
        };

        const toggleTheme = (value) => {
            isDarkTheme.value = Boolean(value);
            applyTheme(isDarkTheme.value);
            saveClientState();
        };

        const serializeMessageForStorage = (msg) => {
            if (!msg || typeof msg !== 'object') return msg;

            return {
                role: msg.role,
                content: msg.content || '',
                steps: Array.isArray(msg.steps) ? msg.steps : [],
                sql: msg.sql || null,
                pythonCode: msg.pythonCode || null,
                needClarification: Boolean(msg.needClarification),
                clarificationSections: Array.isArray(msg.clarificationSections) ? msg.clarificationSections : [],
                reflection: msg.reflection || '',
                reasoning: msg.reasoning || '',
                chartReasoning: msg.chartReasoning || '',
                chartSpec: msg.chartSpec || null,
                sqlResult: msg.sqlResult || null,
                totalCount: msg.totalCount || null,
                isTruncated: Boolean(msg.isTruncated),
                isStreaming: false,
                isChartLoading: false,
                isReplayLoading: false,
                metricPlan: msg.metricPlan || null,
                metricStepStatuses: msg.metricStepStatuses || {},
                metricStepSqls: msg.metricStepSqls || {},
                metricStepResults: msg.metricStepResults || {},
                _expandedSqls: msg._expandedSqls || {}
            };
        };

        const makeThreadSnapshot = (overrides = {}) => ({
            id: createThreadId(),
            sessionId: createSessionId(),
            title: '',
            messages: [],
            suggestedQuestions: [],
            enableSuggestions: enableSuggestions.value,
            updatedAt: Date.now(),
            ...overrides
        });

        const getThreadTitle = (thread) => {
            if (!thread) return '新对话';
            if (thread.title && thread.title.trim()) return thread.title;
            const firstUser = Array.isArray(thread.messages)
                ? thread.messages.find((m) => m?.role === 'user' && m?.content)
                : null;
            if (!firstUser) return '新对话';
            return firstUser.content.length > 18 ? `${firstUser.content.slice(0, 18)}...` : firstUser.content;
        };

        const formatThreadTime = (thread) => {
            const ts = thread?.updatedAt;
            if (!ts) return '刚刚';
            const diffMs = Date.now() - ts;
            const diffMin = Math.floor(diffMs / 60000);
            if (diffMin < 1) return '刚刚';
            if (diffMin < 60) return `${diffMin}m`;
            const diffHour = Math.floor(diffMin / 60);
            if (diffHour < 24) return `${diffHour}h`;
            const diffDay = Math.floor(diffHour / 24);
            return `${diffDay}d`;
        };

        const upsertActiveThreadFromRuntime = () => {
            if (!activeThreadId.value) return;

            const snapshot = makeThreadSnapshot({
                id: activeThreadId.value,
                sessionId,
                title: currentThreadTitle.value || '',
                messages: messages.value
                    .slice(-MAX_PERSIST_MESSAGES)
                    .map((msg) => serializeMessageForStorage(msg)),
                suggestedQuestions: suggestedQuestions.value,
                enableSuggestions: enableSuggestions.value,
                updatedAt: Date.now()
            });

            const idx = threadList.value.findIndex((t) => t.id === activeThreadId.value);
            if (idx >= 0) {
                threadList.value[idx] = snapshot;
            } else {
                threadList.value.unshift(snapshot);
            }

            threadList.value = threadList.value
                .sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0))
                .slice(0, MAX_THREAD_COUNT);
        };

        const saveClientState = () => {
            if (suspendPersist) return;
            if (isAuthenticated()) {
                upsertActiveThreadFromRuntime();
                persistAuthState();
                return;
            }
            try {
                upsertActiveThreadFromRuntime();
                const payload = {
                    version: 2,
                    activeThreadId: activeThreadId.value,
                    threads: threadList.value,
                    uiTheme: isDarkTheme.value ? 'dark' : 'light'
                };
                localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
            } catch (e) {
                console.warn('保存本地会话失败:', e);
            }
        };

        const applyThreadToRuntime = (thread) => {
            suspendPersist = true;
            try {
                activeThreadId.value = thread.id;
                sessionId = thread.sessionId || createSessionId();
                currentThreadTitle.value = thread.title || '';
                messages.value = Array.isArray(thread.messages)
                    ? thread.messages.map((msg) => inflateMessageForRuntime(msg))
                    : [];
                suggestedQuestions.value = Array.isArray(thread.suggestedQuestions) ? thread.suggestedQuestions : [];
                enableSuggestions.value = Boolean(thread.enableSuggestions);
            } finally {
                suspendPersist = false;
            }
            restoreDraft();
        };

        const loadAuthenticatedThread = async (threadId) => {
            const response = await apiFetch(`/api/conversations/${threadId}/messages`);
            if (!response.ok) {
                throw new Error('加载会话历史失败');
            }
            const payload = await response.json();
            const thread = normalizeConversationSummary({
                ...payload.conversation,
                messages: Array.isArray(payload.messages) ? payload.messages : []
            });
            applyThreadSummaryUpdate(thread);
            applyThreadToRuntime(thread);
            persistAuthState();
            scrollToBottom();
        };

        const loadAuthenticatedConversations = async () => {
            const response = await apiFetch('/api/conversations');
            if (!response.ok) {
                throw new Error('加载会话列表失败');
            }
            const payload = await response.json();
            const items = Array.isArray(payload.items) ? payload.items.map((item) => normalizeConversationSummary(item)) : [];
            threadList.value = items;

            if (items.length === 0) {
                const freshThread = makeThreadSnapshot();
                threadList.value = [freshThread];
                applyThreadToRuntime(freshThread);
                persistAuthState();
                return freshThread;
            }

            let nextThreadId = activeThreadId.value;
            try {
                const authState = JSON.parse(localStorage.getItem(AUTH_STORAGE_KEY) || '{}');
                if (!nextThreadId && authState && typeof authState.activeThreadId === 'string') {
                    nextThreadId = authState.activeThreadId;
                }
            } catch (e) {
                console.warn('读取鉴权状态失败:', e);
            }

            const target = items.find((item) => item.id === nextThreadId) || items[0];
            await loadAuthenticatedThread(target.id);
            return target;
        };

        const createNewThread = async () => {
            if (isLoading.value) {
                ElMessage.warning('请等待当前查询完成后再新建对话');
                return;
            }

            if (isAuthenticated()) {
                const freshThread = makeThreadSnapshot();
                threadList.value = [freshThread, ...threadList.value.filter((item) => item.id !== freshThread.id)]
                    .slice(0, MAX_THREAD_COUNT);
                applyThreadToRuntime(freshThread);
                persistAuthState();
                return freshThread;
            }

            upsertActiveThreadFromRuntime();
            const freshThread = makeThreadSnapshot();
            threadList.value.unshift(freshThread);
            applyThreadToRuntime(freshThread);
            saveClientState();
            return freshThread;
        };

        const switchThread = async (threadId) => {
            if (isLoading.value) {
                ElMessage.warning('请等待当前查询完成后再切换对话');
                return;
            }

            if (!threadId || threadId === activeThreadId.value) return;
            if (isAuthenticated()) {
                await loadAuthenticatedThread(threadId);
                return;
            }
            upsertActiveThreadFromRuntime();

            const target = threadList.value.find((t) => t.id === threadId);
            if (!target) return;
            applyThreadToRuntime(target);
            saveClientState();
            scrollToBottom();
        };

        const deleteThread = async (threadId) => {
            if (isLoading.value) {
                ElMessage.warning('请等待当前查询完成后再删除');
                return;
            }
            if (threadList.value.length <= 1) {
                ElMessage.warning('至少保留一个对话');
                return;
            }
            const idx = threadList.value.findIndex((t) => t.id === threadId);
            if (idx < 0) return;

            if (isAuthenticated()) {
                try {
                    await apiFetch(`/api/chat/delete-thread?thread_id=${encodeURIComponent(threadId)}&session_id=${sessionId}`, { method: 'POST' });
                } catch (e) {
                    console.error('删除对话失败:', e);
                }
            }

            threadList.value.splice(idx, 1);

            // 如果删除的是当前活跃对话，切换到最近的对话
            if (threadId === activeThreadId.value) {
                const next = threadList.value[0];
                applyThreadToRuntime(next);
            }
            saveClientState();
        };

        const restoreLegacyClientState = () => {
            try {
                let raw = localStorage.getItem(STORAGE_KEY);
                const legacyRaw = localStorage.getItem(LEGACY_STORAGE_KEY);
                if (raw && legacyRaw && raw === legacyRaw) {
                    localStorage.removeItem(STORAGE_KEY);
                    raw = '';
                }
                if (!raw) {
                    const first = makeThreadSnapshot();
                    threadList.value = [first];
                    applyThreadToRuntime(first);
                    saveClientState();
                    return;
                }

                const payload = JSON.parse(raw);
                if (!payload || typeof payload !== 'object') {
                    throw new Error('invalid payload');
                }

                // v2 多会话结构
                if (Array.isArray(payload.threads)) {
                    if (payload.uiTheme === 'light' || payload.uiTheme === 'dark') {
                        isDarkTheme.value = payload.uiTheme === 'dark';
                        applyTheme(isDarkTheme.value);
                    }

                    const threads = payload.threads
                        .filter((t) => t && typeof t === 'object')
                        .map((t) => makeThreadSnapshot({
                            id: t.id || createThreadId(),
                            sessionId: t.sessionId || createSessionId(),
                            title: t.title || '',
                            messages: Array.isArray(t.messages) ? t.messages : [],
                            suggestedQuestions: Array.isArray(t.suggestedQuestions) ? t.suggestedQuestions : [],
                            enableSuggestions: Boolean(t.enableSuggestions),
                            updatedAt: t.updatedAt || Date.now()
                        }))
                        .slice(0, MAX_THREAD_COUNT);

                    if (threads.length === 0) {
                        const first = makeThreadSnapshot();
                        threadList.value = [first];
                        applyThreadToRuntime(first);
                        saveClientState();
                        return;
                    }

                    threadList.value = threads.sort((a, b) => (b.updatedAt || 0) - (a.updatedAt || 0));
                    const activeId = payload.activeThreadId;
                    const activeThread = threadList.value.find((t) => t.id === activeId) || threadList.value[0];
                    applyThreadToRuntime(activeThread);
                    saveClientState();
                    return;
                }

                // 兼容旧版单会话结构
                if (payload.uiTheme === 'light' || payload.uiTheme === 'dark') {
                    isDarkTheme.value = payload.uiTheme === 'dark';
                    applyTheme(isDarkTheme.value);
                }

                const legacyThread = makeThreadSnapshot({
                    sessionId: typeof payload.sessionId === 'string' && payload.sessionId.trim() ? payload.sessionId : createSessionId(),
                    title: typeof payload.currentThreadTitle === 'string' ? payload.currentThreadTitle : '',
                    messages: Array.isArray(payload.messages) ? payload.messages : [],
                    suggestedQuestions: Array.isArray(payload.suggestedQuestions) ? payload.suggestedQuestions : [],
                    enableSuggestions: typeof payload.enableSuggestions === 'boolean' ? payload.enableSuggestions : false,
                    updatedAt: Date.now()
                });

                threadList.value = [legacyThread];
                applyThreadToRuntime(legacyThread);
                saveClientState();
            } catch (e) {
                console.warn('恢复本地会话失败，已忽略:', e);
                localStorage.removeItem(STORAGE_KEY);
                const first = makeThreadSnapshot();
                threadList.value = [first];
                applyThreadToRuntime(first);
                saveClientState();
            }
        };

        const clearClientState = () => {
            try {
                localStorage.removeItem(STORAGE_KEY);
            } catch (e) {
                console.warn('清理本地会话失败:', e);
            }
        };

        const migrateLegacyGuestState = () => {
            try {
                const currentRaw = localStorage.getItem(STORAGE_KEY);
                if (currentRaw) {
                    return;
                }

                const legacyRaw = localStorage.getItem(LEGACY_STORAGE_KEY);
                if (!legacyRaw) {
                    return;
                }

                const payload = JSON.parse(legacyRaw);
                if (!payload || typeof payload !== 'object') {
                    return;
                }

                const hasThreads = Array.isArray(payload.threads) && payload.threads.length > 0;
                const hasLegacySingleSession = Array.isArray(payload.messages) && payload.messages.length > 0;
                if (!hasThreads && !hasLegacySingleSession) {
                    return;
                }

                localStorage.setItem(STORAGE_KEY, legacyRaw);
            } catch (e) {
                console.warn('迁移旧访客会话失败:', e);
            }
        };

        const openLoginDialog = () => {
            window.location.href = '/login';
        };

        const logout = () => {
            if (isLoading.value) {
                ElMessage.warning('请等待当前查询完成后再退出登录');
                return;
            }

            clearAuthState();
            setAppMode('');
            authToken.value = '';
            currentUser.value = null;
            localStorage.removeItem('hasSkippedLogin');
            window.location.href = '/login';
        };

        const initializeApp = async () => {
            applyTheme(isDarkTheme.value);
            try {
                const appMode = localStorage.getItem(APP_MODE_KEY);
                const skippedLogin = localStorage.getItem('hasSkippedLogin');
                const savedToken = localStorage.getItem(AUTH_TOKEN_KEY) || '';
                const savedUser = localStorage.getItem(AUTH_USER_KEY);
                const authState = JSON.parse(localStorage.getItem(AUTH_STORAGE_KEY) || '{}');
                
                if (authState && (authState.uiTheme === 'light' || authState.uiTheme === 'dark')) {
                    isDarkTheme.value = authState.uiTheme === 'dark';
                    applyTheme(isDarkTheme.value);
                }

                if (appMode === 'guest' || skippedLogin) {
                    clearAuthState();
                    setAppMode('guest');
                    restoreLegacyClientState();
                    return;
                }

                if ((appMode === 'auth' || (!appMode && savedToken && savedUser)) && savedToken && savedUser) {
                    authToken.value = savedToken;
                    currentUser.value = JSON.parse(savedUser);
                    setAppMode('auth');
                    await loadAuthenticatedConversations();
                    return;
                } else if (appMode === 'auth' || !skippedLogin) {
                    window.location.href = '/login';
                    return;
                }
            } catch (e) {
                console.warn('恢复登录态失败:', e);
                clearAuthState();
                if (localStorage.getItem(APP_MODE_KEY) !== 'guest' && !localStorage.getItem('hasSkippedLogin')) {
                    window.location.href = '/login';
                    return;
                }
            }

            restoreLegacyClientState();
        };

        // 格式化 Markdown
        const formatMarkdown = (text) => {
            if (!text) return '';
            return marked.parse(text);
        };

        const CLARIFICATION_PREFIXES = [
            '您是希望',
            '您希望',
            '您想',
            '您更关注',
            '您更想',
            '您要',
            '请您',
            '请'
        ];

        const sanitizeClarificationOption = (text) => {
            let value = (text || '').trim();
            value = value.replace(/^[○●•◦▪·\-*\s]+/, '');
            value = value.replace(/^[,，.。:：;；\-\s]+/, '');
            value = value.replace(/[？?。；;]+$/g, '').trim();

            CLARIFICATION_PREFIXES.forEach((prefix) => {
                if (value.startsWith(prefix)) {
                    value = value.slice(prefix.length).trim();
                }
            });

            if (value.includes('，')) {
                const commaParts = value.split(/[，,]/).map((part) => part.trim()).filter(Boolean);
                if (commaParts.length > 1) {
                    value = commaParts[commaParts.length - 1];
                }
            }

            value = value.replace(/^(以|的是|分析的是|关注的是|想分析的是|想查看的是|想看的|想要的|选择|明确)\s*/, '');
            value = value.replace(/^[a-zA-Z][\)\.、]\s*/, '');
            return value.trim();
        };

        const dedupeOptions = (options) => {
            const seen = new Set();
            return options.filter((option) => {
                if (!option) return false;
                if (seen.has(option)) return false;
                seen.add(option);
                return true;
            });
        };

        const extractLetterOptions = (body) => {
            const markerRegex = /([a-zA-Z])[\)\.、]/g;
            const markers = [];
            let match;
            while ((match = markerRegex.exec(body)) !== null) {
                markers.push({
                    index: match.index,
                    tokenLength: match[0].length
                });
            }

            if (markers.length < 2) return [];

            const options = [];
            for (let i = 0; i < markers.length; i++) {
                const start = markers[i].index + markers[i].tokenLength;
                const end = i + 1 < markers.length ? markers[i + 1].index : body.length;
                const candidate = sanitizeClarificationOption(body.slice(start, end));
                if (candidate) options.push(candidate);
            }

            return dedupeOptions(options);
        };

        const extractClarificationOptions = (body) => {
            const circledMatches = [...body.matchAll(/([①②③④⑤⑥⑦⑧⑨⑩])\s*([^①②③④⑤⑥⑦⑧⑨⑩]+)/g)];
            if (circledMatches.length >= 2) {
                return dedupeOptions(circledMatches.map((match) => sanitizeClarificationOption(match[2])));
            }

            const letterOptions = extractLetterOptions(body);
            if (letterOptions.length >= 2) {
                return letterOptions;
            }

            const numericMatches = [...body.matchAll(/(?:^|\s|[，,、:：])(\d+[.)、])\s*([^\d]+?)(?=(?:\s+\d+[.)、])|$)/g)];
            if (numericMatches.length >= 2) {
                return dedupeOptions(numericMatches.map((match) => sanitizeClarificationOption(match[2])));
            }

            const quotedMatches = [...body.matchAll(/“([^”]+)”/g)];
            if (quotedMatches.length >= 2) {
                return dedupeOptions(quotedMatches.map((match) => sanitizeClarificationOption(match[1])));
            }

            if (body.includes('还是')) {
                const options = dedupeOptions(
                    body
                        .split(/\s*还是\s*/)
                        .map((part) => sanitizeClarificationOption(part))
                );
                return options.length >= 2 ? options : [];
            }

            return [];
        };

        const parseClarificationSections = (text) => {
            if (!text) return [];

            const lines = text
                .split('\n')
                .map((line) => line.trim())
                .filter(Boolean);

            const blocks = [];
            let current = null;

            lines.forEach((line) => {
                const match = line.match(/^(\d+)[.、]\s*(.+)$/);
                if (match) {
                    if (current) blocks.push(current);
                    current = {
                        index: match[1],
                        promptLine: match[2].trim(),
                        bodyLines: []
                    };
                    return;
                }

                if (current) {
                    current.bodyLines.push(line);
                }
            });

            if (current) blocks.push(current);

            return blocks
                .map((block) => {
                    const combinedBody = [block.promptLine, ...block.bodyLines].join('\n');
                    const options = extractClarificationOptions(combinedBody);

                    if (options.length < 2) {
                        return null;
                    }

                    const markerIndex = block.promptLine.search(/[a-zA-Z][\)\.、]|[①②③④⑤⑥⑦⑧⑨⑩]/);
                    let prompt = markerIndex > 0 ? block.promptLine.slice(0, markerIndex).trim() : block.promptLine;
                    prompt = prompt.replace(/(请选择|请您选择|请选择或补充说明)[^。？！?!]*/g, '').trim();
                    if (prompt.endsWith('：') || prompt.endsWith(':')) {
                        prompt = prompt.slice(0, -1).trim();
                    }
                    if (prompt && !/[？?]$/.test(prompt)) {
                        prompt = `${prompt}？`;
                    }

                    return {
                        id: `clarification-${block.index}`,
                        index: block.index,
                        prompt,
                        options,
                        selectedOption: ''
                    };
                })
                .filter(Boolean);
        };

        const buildClarificationDraft = (sections) => {
            if (!Array.isArray(sections)) return '';
            return sections
                .map((section) => {
                    if (!section.selectedOption) return '';
                    return `${section.index}) ${section.selectedOption}`;
                })
                .filter((line) => Boolean(line))
                .join('\n');
        };

        const mergeClarificationDraft = (currentInput, previousDraft, nextDraft) => {
            const current = (currentInput || '').trim();
            const previous = (previousDraft || '').trim();
            const next = (nextDraft || '').trim();

            if (!current || current === previous) {
                return next;
            }

            if (previous && current.includes(previous)) {
                return current.replace(previous, next).trim();
            }

            if (!next) {
                return current;
            }

            return `${current}\n${next}`.trim();
        };

        const lastClarificationDraft = ref('');

        const toggleClarificationOption = (msg, sectionIndex, option) => {
            const section = msg.clarificationSections?.[sectionIndex];
            if (!section) return;

            section.selectedOption = section.selectedOption === option ? '' : option;

            const nextDraft = buildClarificationDraft(msg.clarificationSections);
            inputMessage.value = mergeClarificationDraft(
                inputMessage.value,
                lastClarificationDraft.value,
                nextDraft
            );
            lastClarificationDraft.value = nextDraft;
        };

        const clearClarificationSelection = (msg) => {
            if (!msg?.clarificationSections?.length) return;

            msg.clarificationSections.forEach((section) => {
                section.selectedOption = '';
            });

            inputMessage.value = mergeClarificationDraft(
                inputMessage.value,
                lastClarificationDraft.value,
                ''
            );
            lastClarificationDraft.value = '';
        };

        const submitClarificationSelection = (msg) => {
            if (!msg?.clarificationSections?.length) return;

            const draft = buildClarificationDraft(msg.clarificationSections).trim();
            if (!draft) {
                ElMessage.warning('请先选择至少一个澄清选项');
                return;
            }

            sendMessage(draft);
        };

        const toggleMetricSql = (msg, stepId) => {
            if (!msg._expandedSqls) {
                msg._expandedSqls = {};
            }
            msg._expandedSqls[stepId] = !msg._expandedSqls[stepId];
        };

        const copySql = (sql) => {
            if (!sql) return;
            navigator.clipboard.writeText(sql).then(() => {
                ElMessage.success('SQL 已复制');
            }).catch(() => {
                ElMessage.error('复制失败');
            });
        };

        // 发送消息
        const handleSend = () => {
            sendMessage(inputMessage.value);
        };

        const handleEnter = (e) => {
            if (!e.shiftKey) {
                handleSend();
            }
        };

        // 用于取消请求的控制器
        let currentAbortController = null;

        // 停止查询
        const stopQuery = () => {
            if (currentAbortController) {
                currentAbortController.abort();
                currentAbortController = null;
            }
            isLoading.value = false;

            // 更新最后一条消息状态
            if (messages.value.length > 0) {
                const lastMsg = messages.value[messages.value.length - 1];
                if (lastMsg.role === 'assistant' && lastMsg.isStreaming) {
                    lastMsg.isStreaming = false;
                    lastMsg.content = lastMsg.content || '查询已被用户中止';
                }
            }
        };

        const sendMessage = async (content) => {
            if (!content.trim() || isLoading.value || planReviewVisible.value) return;
            if (isAuthenticated() && !activeThreadId.value) {
                await createNewThread();
            }
            const pendingConversationId = activeThreadId.value || createThreadId();
            if (!activeThreadId.value) {
                activeThreadId.value = pendingConversationId;
            }

            // 添加用户消息
            messages.value.push({ role: 'user', content: content });
            inputMessage.value = '';
            lastClarificationDraft.value = '';
            isLoading.value = true;
            currentSteps.value = [];
            clearDraft();

            // 设置标题
            if (!currentThreadTitle.value) {
                currentThreadTitle.value = content.length > 10 ? content.slice(0, 10) + '...' : content;
            }

            scrollToBottom();

            // 创建新的 AbortController
            currentAbortController = new AbortController();

            messages.value.push({
                role: 'assistant',
                content: '',
                steps: [],
                sql: null,
                pythonCode: null,
                sqlResult: null,
                isStreaming: true,
                activeCollapse: ['1'],
                reasoning: '',
                reflection: '',
                isChartLoading: false,
                isReplayLoading: false,
                metricPlan: null,
                metricStepStatuses: {},
                metricStepSqls: {},
                metricStepResults: {},
                _expandedSqls: {}
            });
            const tempMessage = messages.value[messages.value.length - 1];

            try {
                // 使用流式 API
                const url = '/api/chat/stream';
                const body = {
                    message: content,
                    session_id: sessionId,
                    workspace_id: threadList.value.find(t => t.id === activeThreadId.value)?.workspaceId || undefined,
                    thread_id: activeThreadId.value,
                    enable_suggestions: enableSuggestions.value
                };
                const response = await apiFetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                    signal: currentAbortController.signal
                });

                if (!response.ok) throw new Error('API 请求失败');

                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = '';

                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;

                    buffer += decoder.decode(value, { stream: true });
                    const lines = buffer.split('\n');
                    buffer = lines.pop(); // 保留未完成的行

                    for (const line of lines) {
                        if (line.startsWith('data: ')) {
                            const data = line.slice(6);

                            if (data === '[DONE]') {
                                // 流式完成
                                tempMessage.isStreaming = false;
                                break;
                            }

                            try {
                                const event = JSON.parse(data);

                                if (event.type === 'start') {
                                    // 开始处理
                                    console.log('Start:', event.message);
                                } else if (event.type === 'step') {
                                    // 更新步骤
                                    const stepInfo = {
                                        title: event.title,
                                        desc: event.detail || event.message
                                    };

                                    // 如果是相同节点，更新；否则添加
                                    const existingIndex = tempMessage.steps.findIndex(s => s.title === event.title);
                                    if (existingIndex >= 0) {
                                        tempMessage.steps[existingIndex] = stepInfo;
                                    } else {
                                        tempMessage.steps.push(stepInfo);
                                    }

                                    if (event.sql) {
                                        tempMessage.sql = event.sql;
                                    }

                                    // 指标计划数据：首次收到 metric_plan 时初始化计划面板
                                    if (event.metric_plan && Array.isArray(event.metric_plan) && event.metric_plan.length > 0) {
                                        console.log('[metric] 收到 metric_plan 事件，节点数:', event.metric_plan.length, '当前节点:', event.node);
                                        if (!tempMessage.metricPlan) {
                                            // 首次初始化，过滤无效节点
                                            const validNodes = event.metric_plan.filter(n => n && n.step_id);
                                            tempMessage.metricPlan = validNodes;
                                            tempMessage.metricStepStatuses = {};
                                            tempMessage.metricStepSqls = {};
                                            tempMessage.metricStepResults = {};
                                            validNodes.forEach((node) => {
                                                tempMessage.metricStepStatuses[node.step_id] = 'pending';
                                            });
                                            triggerRef(messages);
                                            console.log('[metric] metricPlan 已初始化，步骤状态:', JSON.stringify(tempMessage.metricStepStatuses));
                                        } else {
                                            // 计划更新（如调整后）：保留已有步骤进度，新步骤标记 pending
                                            const validNodes = event.metric_plan.filter(n => n && n.step_id);
                                            tempMessage.metricPlan = validNodes;
                                            validNodes.forEach((node) => {
                                                if (!tempMessage.metricStepStatuses[node.step_id]) {
                                                    tempMessage.metricStepStatuses[node.step_id] = 'pending';
                                                }
                                            });
                                            triggerRef(messages);
                                            console.log('[metric] metricPlan 已更新（计划调整），保留已有进度');
                                        }
                                    }

                                    // 指标步骤关联：将事件映射到具体计划步骤
                                    const mSid = event.metric_step_id || '';
                                    if (mSid && tempMessage.metricPlan) {
                                        console.log('[metric] 步骤事件:', event.node, 'step_id:', mSid, 'status:', event.metric_step_status);
                                        // metric_sql_generator → 标记 running + 存 SQL
                                        if (event.node === 'metric_sql_generator') {
                                            tempMessage.metricStepStatuses[mSid] = 'running';
                                            if (event.metric_step_sql || event.sql) {
                                                tempMessage.metricStepSqls[mSid] = event.metric_step_sql || event.sql;
                                            }
                                            triggerRef(messages);
                                        }
                                        // metric_executor → 更新结果摘要
                                        if (event.node === 'metric_executor') {
                                            if (event.metric_step_result) {
                                                tempMessage.metricStepResults[mSid] = event.metric_step_result;
                                            }
                                            if (event.metric_step_error) {
                                                tempMessage.metricStepStatuses[mSid] = 'failed';
                                                tempMessage.metricStepResults[mSid] = { error: event.metric_step_error };
                                            }
                                            triggerRef(messages);
                                        }
                                        // metric_observer → 更新步骤状态
                                        if (event.node === 'metric_observer') {
                                            const obsStatus = event.metric_step_status || '';
                                            if (obsStatus === 'succeeded') {
                                                tempMessage.metricStepStatuses[mSid] = 'succeeded';
                                            } else if (obsStatus.startsWith('failed')) {
                                                tempMessage.metricStepStatuses[mSid] = 'failed';
                                            } else if (obsStatus === 'running') {
                                                tempMessage.metricStepStatuses[mSid] = 'running';
                                            }
                                            triggerRef(messages);
                                        }
                                        // metric_loop_planner 在重试时 → 标记 running
                                        if (event.node === 'metric_loop_planner' && tempMessage.metricStepStatuses[mSid] === 'failed') {
                                            tempMessage.metricStepStatuses[mSid] = 'running';
                                            triggerRef(messages);
                                        }
                                    }

                                    // 处理流式推理和反思
                                    if (event.reasoning) {
                                        typeWriter(tempMessage, 'reasoning', event.reasoning);
                                    }
                                    if (event.reflection) {
                                        typeWriter(tempMessage, 'reflection', event.reflection);
                                    }

                                } else if (event.type === 'result') {
                                    // 最终结果
                                    tempMessage.content = event.response;
                                    tempMessage.needClarification = Boolean(event.need_clarification);
                                    tempMessage.clarificationSections = event.need_clarification
                                        ? parseClarificationSections(event.response)
                                        : [];
                                    if (event.sql) {
                                        tempMessage.sql = event.sql;
                                    }
                                    if (event.python_code) {
                                        // 新增: 处理 Python 代码
                                        tempMessage.pythonCode = event.python_code;
                                    }
                                    if (event.sql_reflection) {
                                        // 最终结果中如果还有反思，确保显示（通常步骤中已经流式显示了）
                                        if (!tempMessage.reflection) {
                                            typeWriter(tempMessage, 'reflection', event.sql_reflection);
                                        }
                                    }

                                    if (Object.prototype.hasOwnProperty.call(event, 'data')) {
                                        tempMessage.sqlResult = event.data;
                                        tempMessage.totalCount = event.total_count || 0;
                                        tempMessage.isTruncated = event.is_truncated || false;
                                    }

                                    // 如果有推荐问题，更新全局推荐列表
                                    if (event.suggested_questions) {
                                        suggestedQuestions.value = event.suggested_questions;
                                    }

                                    tempMessage.isStreaming = false;
                                    triggerRef(messages);
                                } else if (event.type === 'plan_review') {
                                    // 计划审核事件 - 弹窗展示给用户
                                    planReviewVisible.value = true;
                                    planReviewData.value = event.plan_nodes || [];
                                    planReviewAdjustments.value = '';
                                } else if (event.type === 'error') {
                                    // 错误
                                    tempMessage.content = '系统错误：' + event.message;
                                    tempMessage.isStreaming = false;
                                }

                            } catch (e) {
                                console.error('解析事件失败:', e, data);
                            }
                        }
                    }

                    scrollToBottom();
                }

            } catch (error) {
                // 检查是否是用户主动中止
                if (error.name === 'AbortError') {
                    console.log('查询已被用户中止');
                    // 更新最后一条消息
                    if (messages.value.length > 0) {
                        const lastMsg = messages.value[messages.value.length - 1];
                        if (lastMsg.role === 'assistant') {
                            lastMsg.isStreaming = false;
                            if (!lastMsg.content) {
                                lastMsg.content = '查询已被用户中止';
                            }
                        }
                    }
                } else {
                    // 其他错误：移除临时消息并添加错误消息
                    messages.value.pop();
                    messages.value.push({
                        role: 'assistant',
                        content: '抱歉，系统出现错误：' + error.message
                    });
                }
            } finally {
                isLoading.value = false;
                currentAbortController = null;
                if (isAuthenticated()) {
                    try {
                        await saveAuthenticatedHistory(pendingConversationId, [
                            { role: 'user', content },
                            tempMessage
                        ]);
                    } catch (e) {
                        ElMessage.error(`保存历史失败：${e.message || e}`);
                    }
                }
                upsertActiveThreadFromRuntime();
                saveClientState();
                scrollToBottom();
            }
        };

        // 清空当前对话（保留历史线程）
        const resetChat = async () => {
            if (isAuthenticated()) {
                try {
                    await fetch(`/api/reset?session_id=${sessionId}`, { method: 'POST' });
                    sessionId = createSessionId();
                    messages.value = [];
                    currentThreadTitle.value = '';
                    suggestedQuestions.value = [];
                    saveClientState();
                    ElMessage.success('当前会话已清空，历史记录仍可查看');
                } catch (e) {
                    ElMessage.error(`重置失败：${e.message || e}`);
                }
                return;
            }

            try {
                await fetch(`/api/reset?session_id=${sessionId}`, { method: 'POST' });
            } catch (e) {
                console.error(e);
            }
            sessionId = createSessionId();
            messages.value = [];
            currentThreadTitle.value = '';
            suggestedQuestions.value = [];
            saveClientState();
        };

        const scrollToBottom = () => {
            nextTick(() => {
                if (chatWrapper.value) {
                    chatWrapper.value.scrollTop = chatWrapper.value.scrollHeight;
                }
            });
        };

        // 生成图表
        const generateChart = async (msg, index) => {
            if (msg.isChartLoading) return;
            msg.isChartLoading = true;

            try {
                const response = await fetch('/api/chart', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        session_id: sessionId
                    })
                });

                const result = await response.json();

                if (result.chart_spec) {
                    msg.chartSpec = result.chart_spec;
                    msg.chartReasoning = result.chart_reasoning;

                    // 自动打开图表弹窗
                    viewChart(msg.chartSpec, msg.chartReasoning);
                    scrollToBottom();
                } else {
                    ElMessage.warning(result.reasoning || '无法生成相关图表');
                }

            } catch (e) {
                console.error('生成图表失败', e);
                ElMessage.error('生成图表失败: ' + e.message);
            } finally {
                msg.isChartLoading = false;
            }
        };

        watch(messages, saveClientState, { deep: true });
        watch(currentThreadTitle, saveClientState);
        watch(suggestedQuestions, saveClientState, { deep: true });
        watch(enableSuggestions, saveClientState);
        watch(inputMessage, (val) => {
            saveDraft(val);
        });
        watch(isDarkTheme, (val) => {
            applyTheme(Boolean(val));
            saveClientState();
        });

        onMounted(() => {
            initializeApp();
            window.addEventListener('beforeunload', saveClientState);
            scrollToBottom();
        });

        onBeforeUnmount(() => {
            window.removeEventListener('beforeunload', saveClientState);
        });

        // 加载指标体系数据
        const loadMetricsData = async () => {
            if (metricsData.value) return;
            metricsLoading.value = true;
            try {
                const response = await fetch('/api/metrics');
                if (response.ok) {
                    const data = await response.json();
                    metricsData.value = data;
                    // 默认全部展开
                    Object.keys(data).forEach(key => {
                        metricsExpanded[key] = true;
                    });
                } else {
                    ElMessage.error('加载指标体系失败');
                }
            } catch (e) {
                console.error('加载指标体系失败:', e);
                ElMessage.error('加载指标体系失败');
            } finally {
                metricsLoading.value = false;
            }
        };

        const toggleMetricsLevel1 = (name) => {
            metricsExpanded[name] = !metricsExpanded[name];
        };

        // 加载数据库Schema
        const loadSchemaData = async () => {
            if (schemaData.value) return;
            schemaLoading.value = true;
            try {
                const response = await fetch('/api/schema');
                if (response.ok) {
                    const data = await response.json();
                    schemaData.value = data;
                } else {
                    ElMessage.error('加载数据库结构失败');
                }
            } catch (e) {
                console.error('加载数据库结构失败:', e);
                ElMessage.error('加载数据库结构失败');
            } finally {
                schemaLoading.value = false;
            }
        };

        // 切换视图
        const switchView = (view) => {
            currentView.value = view;
            if (view === 'modeling') {
                loadMetricsData();
            } else if (view === 'database') {
                loadSchemaData();
            }
        };

        // 计划审核相关函数
        const _handleSSEEvent = (event, tempMessage) => {
            if (event.type === 'start') {
                console.log('Start:', event.message);
            } else if (event.type === 'step') {
                const stepInfo = {
                    title: event.title,
                    desc: event.detail || event.message
                };
                const existingIndex = tempMessage.steps.findIndex(s => s.title === event.title);
                if (existingIndex >= 0) {
                    tempMessage.steps[existingIndex] = stepInfo;
                } else {
                    tempMessage.steps.push(stepInfo);
                }
                if (event.sql) {
                    tempMessage.sql = event.sql;
                }
                if (event.metric_plan && Array.isArray(event.metric_plan) && event.metric_plan.length > 0) {
                    const validNodes = event.metric_plan.filter(n => n && n.step_id);
                    if (!tempMessage.metricPlan) {
                        tempMessage.metricPlan = validNodes;
                        tempMessage.metricStepStatuses = {};
                        tempMessage.metricStepSqls = {};
                        tempMessage.metricStepResults = {};
                        validNodes.forEach((node) => {
                            tempMessage.metricStepStatuses[node.step_id] = 'pending';
                        });
                    } else {
                        tempMessage.metricPlan = validNodes;
                        validNodes.forEach((node) => {
                            if (!tempMessage.metricStepStatuses[node.step_id]) {
                                tempMessage.metricStepStatuses[node.step_id] = 'pending';
                            }
                        });
                    }
                    triggerRef(messages);
                }
                const mSid = event.metric_step_id || '';
                if (mSid && tempMessage.metricPlan) {
                    if (event.node === 'metric_sql_generator') {
                        tempMessage.metricStepStatuses[mSid] = 'running';
                        if (event.metric_step_sql || event.sql) {
                            tempMessage.metricStepSqls[mSid] = event.metric_step_sql || event.sql;
                        }
                        triggerRef(messages);
                    }
                    if (event.node === 'metric_executor') {
                        if (event.metric_step_result) {
                            tempMessage.metricStepResults[mSid] = event.metric_step_result;
                        }
                        if (event.metric_step_error) {
                            tempMessage.metricStepStatuses[mSid] = 'failed';
                            tempMessage.metricStepResults[mSid] = { error: event.metric_step_error };
                        }
                        triggerRef(messages);
                    }
                    if (event.node === 'metric_observer') {
                        const obsStatus = event.metric_step_status || '';
                        if (obsStatus === 'succeeded') {
                            tempMessage.metricStepStatuses[mSid] = 'succeeded';
                        } else if (obsStatus.startsWith('failed')) {
                            tempMessage.metricStepStatuses[mSid] = 'failed';
                        } else if (obsStatus === 'running') {
                            tempMessage.metricStepStatuses[mSid] = 'running';
                        }
                        triggerRef(messages);
                    }
                    if (event.node === 'metric_loop_planner' && tempMessage.metricStepStatuses[mSid] === 'failed') {
                        tempMessage.metricStepStatuses[mSid] = 'running';
                        triggerRef(messages);
                    }
                }
                if (event.reasoning) {
                    typeWriter(tempMessage, 'reasoning', event.reasoning);
                }
                if (event.reflection) {
                    typeWriter(tempMessage, 'reflection', event.reflection);
                }
            } else if (event.type === 'result') {
                tempMessage.content = event.response;
                tempMessage.needClarification = Boolean(event.need_clarification);
                tempMessage.clarificationSections = event.need_clarification
                    ? parseClarificationSections(event.response)
                    : [];
                if (event.sql) tempMessage.sql = event.sql;
                if (event.python_code) tempMessage.pythonCode = event.python_code;
                if (event.sql_reflection && !tempMessage.reflection) {
                    typeWriter(tempMessage, 'reflection', event.sql_reflection);
                }
                if (Object.prototype.hasOwnProperty.call(event, 'data')) {
                    tempMessage.sqlResult = event.data;
                    tempMessage.totalCount = event.total_count || 0;
                    tempMessage.isTruncated = event.is_truncated || false;
                }
                if (event.suggested_questions) {
                    suggestedQuestions.value = event.suggested_questions;
                }
                tempMessage.isStreaming = false;
                triggerRef(messages);
            } else if (event.type === 'plan_review') {
                planReviewVisible.value = true;
                planReviewData.value = event.plan_nodes || [];
                planReviewAdjustments.value = '';
            } else if (event.type === 'error') {
                tempMessage.content = '系统错误：' + event.message;
                tempMessage.isStreaming = false;
            }
        };

        const _readSSEStream = async (response, tempMessage) => {
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split('\n');
                buffer = lines.pop();

                for (const line of lines) {
                    if (line.startsWith('data: ')) {
                        const data = line.slice(6);
                        if (data === '[DONE]') {
                            tempMessage.isStreaming = false;
                            break;
                        }
                        try {
                            const event = JSON.parse(data);
                            _handleSSEEvent(event, tempMessage);
                        } catch (e) {
                            console.error('解析事件失败:', e, data);
                        }
                    }
                }
                scrollToBottom();
            }
        };

        const _makeAssistantMessage = () => ({
            role: 'assistant',
            content: '',
            steps: [],
            sql: null,
            pythonCode: null,
            sqlResult: null,
            isStreaming: true,
            activeCollapse: ['1'],
            reasoning: '',
            reflection: '',
            isChartLoading: false,
            isReplayLoading: false,
            metricPlan: null,
            metricStepStatuses: {},
            metricStepSqls: {},
            metricStepResults: {},
            _expandedSqls: {}
        });

        const submitPlanReviewDecision = async (approved, adjustments = '') => {
            planReviewVisible.value = false;

            isLoading.value = true;
            currentSteps.value = [];

            scrollToBottom();
            currentAbortController = new AbortController();

            // 复用最后一条 assistant 消息（不创建新对话框）
            const tempMessage = messages.value.length > 0
                ? messages.value[messages.value.length - 1]
                : null;
            if (!tempMessage || tempMessage.role !== 'assistant') {
                messages.value.push(_makeAssistantMessage());
            }
            const msgRef = messages.value[messages.value.length - 1];
            msgRef.isStreaming = true;
            msgRef.content = '';
            msgRef.steps = [];
            msgRef.sql = null;
            msgRef.pythonCode = null;
            msgRef.sqlResult = null;

            try {
                const body = {
                    session_id: sessionId,
                    workspace_id: threadList.value.find(t => t.id === activeThreadId.value)?.workspaceId || undefined,
                    thread_id: activeThreadId.value,
                    approved: approved,
                    adjustments: adjustments
                };
                const response = await apiFetch('/api/chat/resume-plan', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                    signal: currentAbortController.signal
                });

                if (!response.ok) {
                    const errPayload = await response.json().catch(() => ({}));
                    throw new Error(errPayload.detail || 'API 请求失败');
                }
                await _readSSEStream(response, msgRef);

            } catch (error) {
                if (error.name === 'AbortError') {
                    if (messages.value.length > 0) {
                        const lastMsg = messages.value[messages.value.length - 1];
                        if (lastMsg.role === 'assistant') {
                            lastMsg.isStreaming = false;
                            if (!lastMsg.content) lastMsg.content = '查询已被用户中止';
                        }
                    }
                } else {
                    messages.value.pop();
                    messages.value.push({
                        role: 'assistant',
                        content: '抱歉，系统出现错误：' + error.message
                    });
                }
            } finally {
                isLoading.value = false;
                currentAbortController = null;
                upsertActiveThreadFromRuntime();
                saveClientState();
                scrollToBottom();
            }
        };

        const approvePlanReview = () => {
            submitPlanReviewDecision(true);
        };

        const rejectPlanReview = () => {
            const adjustments = planReviewAdjustments.value.trim();
            if (!adjustments) {
                ElMessage.warning('请输入调整描述');
                return;
            }
            submitPlanReviewDecision(false, adjustments);
        };

        const cancelPlanReview = async () => {
            planReviewVisible.value = false;
            planReviewData.value = null;
            planReviewAdjustments.value = '';
            try {
                const wsId = threadList.value.find(t => t.id === activeThreadId.value)?.workspaceId || '';
                await fetch(`/api/chat/cancel-plan?session_id=${sessionId}&workspace_id=${encodeURIComponent(wsId)}&thread_id=${encodeURIComponent(activeThreadId.value)}`, { method: 'POST' });
            } catch (e) {
                console.error('取消计划审核失败:', e);
            }
        };

        return {
            messages,
            inputMessage,
            isLoading,
            chatWrapper,
            threadList,
            activeThreadId,
            currentThreadTitle,
            suggestedQuestions,
            authToken,
            currentUser,
            openLoginDialog,
            logout,
            isDarkTheme,
            toggleTheme,
            getThreadTitle,
            formatThreadTime,
            createNewThread,
            switchThread,
            deleteThread,
            startResizeSidebar,
            hasResultData,
            hasReplaySource,
            canViewData,
            canGenerateChart,
            handleSend,
            handleEnter,
            sendMessage,
            stopQuery,  // 新增: 停止查询
            resetChat,
            formatMarkdown,
            dialogVisible,
            tableData,
            nestedTableSections,
            showData,
            sqlDialogVisible,
            currentSql,
            showSql,
            chartDialogVisible,
            currentChartSpec,
            currentChartReasoning,
            viewChart,
            enableSuggestions,
            generateChart,
            toggleClarificationOption,
            clearClarificationSelection,
            submitClarificationSelection,
            toggleMetricSql,
            copySql,
            // 视图相关
            currentView,
            switchView,
            // 建模页面
            metricsData,
            metricsLoading,
            metricsExpanded,
            loadMetricsData,
            toggleMetricsLevel1,
            // 数据库页面
            schemaData,
            schemaLoading,
            loadSchemaData,
            // 计划审核
            planReviewVisible,
            planReviewData,
            planReviewAdjustments,
            approvePlanReview,
            rejectPlanReview,
            cancelPlanReview
        };
    }
});

app.use(ElementPlus);
app.mount('#app');
