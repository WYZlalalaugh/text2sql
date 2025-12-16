const { createApp, ref, reactive, nextTick, onMounted } = Vue;

const app = createApp({
    setup() {
        // 状态定义
        const messages = ref([]);
        const inputMessage = ref('');
        const isLoading = ref(false);
        const chatWrapper = ref(null);
        let sessionId = 'session_' + Date.now();
        const currentThreadTitle = ref('');

        // 当前回答的步骤（用于流式显示）
        const currentSteps = ref([]);

        // 推荐问题
        const suggestedQuestions = ref([
            '查询湖北省不同地市的学校数量',
            '对比2023年各类型学校的教师数',
            '分析数字素养指标得分情况'
        ]);

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

            try {
                // 使用流式 API
                const response = await fetch('/api/chat/stream', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        message: content,
                        session_id: sessionId
                    })
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

                                } else if (event.type === 'result') {
                                    // 最终结果
                                    tempMessage.content = event.response;
                                    if (event.sql) {
                                        tempMessage.sql = event.sql;
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
                // 移除临时消息并添加错误消息
                messages.value.pop();
                messages.value.push({
                    role: 'assistant',
                    content: '抱歉，系统出现错误：' + error.message
                });
            } finally {
                isLoading.value = false;
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
            resetChat,
            formatMarkdown
        };
    }
});

app.use(ElementPlus);
app.mount('#app');
