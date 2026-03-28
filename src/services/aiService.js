const OpenAI = require('openai');
const faqs = require('../data/faqs');
const { orders, accounts } = require('../data/mockOrders');

const client = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

const SYSTEM_PROMPT = `You are a helpful customer support agent for an e-commerce company.
You help customers with:
- Frequently asked questions about shipping, returns, payments, and account management
- Order status lookups
- Account information

FAQ Knowledge Base:
${faqs.map(f => `Q: ${f.question}\nA: ${f.answer}`).join('\n\n')}

Order Data (reference only when customer provides an order ID):
${Object.values(orders).map(o => `Order ${o.id}: Status=${o.status}, Items=${o.items.join(', ')}`).join('\n')}

Rules:
- Be concise, friendly, and professional
- If a customer seems very frustrated, upset, or asks to speak with a human, include EXACTLY this phrase in your response: "[ESCALATE]"
- Include [ESCALATE] when: customer uses words like "furious", "terrible", "worst", "lawsuit", "scam", "fraud", "escalate", "manager", "supervisor", "human agent", or expresses severe dissatisfaction multiple times
- Always try to resolve the issue before escalating
- Never make up order information — only use what's in the data above
- For account questions, ask for the customer's email to look up their account`;

const ESCALATION_PHRASES = [
  'speak to a human',
  'talk to a person',
  'real agent',
  'human agent',
  'manager',
  'supervisor',
  'escalate',
  'this is unacceptable',
  'terrible service',
  'worst experience',
  'lawsuit',
  'fraud',
  'scam',
  'furious',
  'absolutely ridiculous'
];

function detectEscalationIntent(message) {
  const lower = message.toLowerCase();
  return ESCALATION_PHRASES.some(phrase => lower.includes(phrase));
}

function lookupOrderStatus(message) {
  const match = message.match(/ORD-\d+/i);
  if (match) {
    const order = orders[match[0].toUpperCase()];
    if (order) return order;
  }
  return null;
}

function lookupAccount(message) {
  const emailMatch = message.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
  if (emailMatch) {
    return accounts[emailMatch[0].toLowerCase()] || null;
  }
  return null;
}

async function getChatResponse(conversationHistory, userMessage) {
  const userRequestsEscalation = detectEscalationIntent(userMessage);

  const orderInfo = lookupOrderStatus(userMessage);
  const accountInfo = lookupAccount(userMessage);

  let contextAddendum = '';
  if (orderInfo) {
    contextAddendum += `\n\nCurrent order lookup: ${JSON.stringify(orderInfo)}`;
  }
  if (accountInfo) {
    contextAddendum += `\n\nCurrent account lookup: ${JSON.stringify(accountInfo)}`;
  }
  if (userRequestsEscalation) {
    contextAddendum += '\n\nNote: Customer has explicitly requested to speak with a human. Include [ESCALATE] in your response.';
  }

  const systemMessage = SYSTEM_PROMPT + contextAddendum;

  const messages = [
    { role: 'system', content: systemMessage },
    ...conversationHistory,
    { role: 'user', content: userMessage }
  ];

  const response = await client.chat.completions.create({
    model: 'gpt-4o-mini',
    messages,
    max_tokens: 500,
    temperature: 0.7
  });

  const assistantMessage = response.choices[0].message.content;
  const shouldEscalate = assistantMessage.includes('[ESCALATE]') || userRequestsEscalation;
  const cleanedMessage = assistantMessage.replace('[ESCALATE]', '').trim();

  return {
    message: cleanedMessage,
    shouldEscalate,
    usage: response.usage
  };
}

module.exports = { getChatResponse };
