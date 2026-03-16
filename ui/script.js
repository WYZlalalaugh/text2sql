const { createApp, ref, reactive, nextTick, onMounted } = Vue;
const { ElMessage } = ElementPlus;

const app = createApp({
    setup() {
        // 状态定义
        const messages = ref([]);
        const inputMessage = ref('');
        const isLoading = ref(false);
        const chatWrapper = ref(null);
        let sessionId = 'session_' + Date.now();
        const currentThreadTitle = ref('');
        const enableSuggestions = ref(false); // 默认关闭开关

        // 当前回答的步骤（用于流式显示）
        const currentSteps = ref([]);

        // 数据展示弹窗
        const dialogVisible = ref(false);
        const tableData = ref([]);

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


        const showData = (data) => {
            tableData.value = data;
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

            // 使用 setTimeout 确保 Dialog DOM 已渲染
            setTimeout(() => {
                try {
                    // 关键修复：移除 Vue 的响应式代理，传递纯 JSON 对象给 Vega
                    const rawSpec = JSON.parse(JSON.stringify(spec));

                    // 强制让图表充满容器
                    rawSpec.width = "container";
                    rawSpec.height = "container";
                    rawSpec.autosize = { type: "fit", contains: "padding" };

                    // 确保容器存在
                    const container = document.querySelector('#dialog-chart-container');
                    if (container) {
                        vegaEmbed('#dialog-chart-container', rawSpec, {
                            actions: false,
                            renderer: 'svg'
                        }).catch(e => {
                            console.error('Vega Embed Error:', e);
                            ElMessage.error('图表渲染出错');
                        });
                    } else {
                        console.error('Chart container not found');
                    }
                } catch (e) {
                    console.error('Chart view error:', e);
                }
            }, 100);
        };

        // 推荐问题（动态获取）
        const suggestedQuestions = ref([]);

        // 格式化 Markdown
        const formatMarkdown = (text) => {
            if (!text) return '';
            return marked.parse(text);
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
            if (!content.trim() || isLoading.value) return;

            // 添加用户消息
            messages.value.push({ role: 'user', content: content });
            inputMessage.value = '';
            isLoading.value = true;
            currentSteps.value = [];

            // 设置标题
            if (!currentThreadTitle.value) {
                currentThreadTitle.value = content.length > 10 ? content.slice(0, 10) + '...' : content;
            }

            scrollToBottom();

            // 创建新的 AbortController
            currentAbortController = new AbortController();

            try {
                // 使用流式 API
                const response = await fetch('/api/chat/stream', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        message: content,
                        session_id: sessionId,
                        enable_suggestions: enableSuggestions.value
                    }),
                    signal: currentAbortController.signal  // 添加取消信号
                });

                if (!response.ok) throw new Error('API 请求失败');

                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = '';

                // 创建一个临时的助手消息对象（用于显示正在生成的步骤）
                const tempMessage = {
                    role: 'assistant',
                    content: '',
                    steps: [],
                    sql: null,
                    sqlResult: null,
                    isStreaming: true
                };
                messages.value.push(tempMessage);

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

                                    if (event.data) {
                                        tempMessage.sqlResult = event.data;
                                    }

                                    // 如果有推荐问题，更新全局推荐列表
                                    if (event.suggested_questions) {
                                        suggestedQuestions.value = event.suggested_questions;
                                    }

                                    tempMessage.isStreaming = false;
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
                scrollToBottom();
            }
        };

        // 重置对话
        const resetChat = async () => {
            try {
                await fetch(`/api/reset?session_id=${sessionId}`, { method: 'POST' });
            } catch (e) {
                console.error(e);
            }
            sessionId = 'session_' + Date.now();
            messages.value = [];
            currentThreadTitle.value = '';
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

        return {
            messages,
            inputMessage,
            isLoading,
            chatWrapper,
            currentThreadTitle,
            suggestedQuestions,
            handleSend,
            handleEnter,
            sendMessage,
            stopQuery,  // 新增: 停止查询
            resetChat,
            formatMarkdown,
            dialogVisible,
            tableData,
            showData,
            sqlDialogVisible,
            currentSql,
            showSql,
            chartDialogVisible,
            currentChartSpec,
            currentChartReasoning,
            viewChart,
            enableSuggestions,
            generateChart
        };
    }
});

app.use(ElementPlus);
app.mount('#app');
