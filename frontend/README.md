# 股票分析系统 - 前端

基于React + UmiJS的股票分析系统前端应用。

## 🚀 快速开始

### 安装依赖
```bash
npm install
```

### 启动开发服务器
```bash
npm run dev
```

访问地址：`http://localhost:8000`

### 构建生产版本
```bash
npm run build
```

## 📍 路由地址

- 首页：`http://localhost:8000/#/home`
- 股票详情：`http://localhost:8000/#/stock/{股票代码}`
  - 示例：`http://localhost:8000/#/stock/000001`

## ⚙️ API配置

### 环境变量配置

项目支持通过环境变量配置API域名：

- **开发环境** (`.env`)：
  ```
  REACT_APP_API_BASE_URL=http://localhost:8888
  ```

- **生产环境** (`.env.production`)：
  ```
  REACT_APP_API_BASE_URL=https://your-production-domain.com
  ```

### 修改API域名

1. **方法一：修改环境变量文件**
   ```bash
   # 编辑 .env 文件
   REACT_APP_API_BASE_URL=http://your-new-domain:port
   ```

2. **方法二：直接修改配置文件**
   ```typescript
   // src/config/api.ts
   export const API_CONFIG = {
     BASE_URL: 'http://your-new-domain:port',
     // ...其他配置
   };
   ```

### API接口列表

- `GET /api/stock_data` - 获取股票数据
- `POST /api/start_stock_analysis` - 开始股票分析
- `GET /api/analysis_status/{task_id}` - 查询分析状态
- `POST /api/cancel_analysis/{task_id}` - 取消分析任务

## 🏗️ 项目结构

```
src/
├── config/
│   └── api.ts              # API配置文件
├── pages/
│   └── Stock/
│       └── Detail/
│           ├── index.tsx   # 股票详情页面
│           └── styles.css  # 页面样式
├── utils/
│   ├── types.ts           # TypeScript类型定义
│   └── formatters.ts      # 格式化工具函数
└── .umirc.ts              # UmiJS配置文件
```

## 🔧 开发说明

### 技术栈
- React 18
- UmiJS 4
- TypeScript
- CSS3

### 特性
- 📱 响应式设计
- 🔄 实时数据轮询
- 📊 图表占位符（待集成ApexCharts）
- 🎨 现代化UI设计
- ⚡ TypeScript类型安全
- 🛡️ 错误处理机制

## 📊 图表功能

### 蜡烛图 + MA均线
- **蜡烛图**：显示股票的开盘价、收盘价、最高价、最低价
- **MA均线**：MA5、MA20、MA60 移动平均线
- **交互功能**：缩放、平移、悬停提示
- **颜色规范**：红涨绿跌（中国股市习惯）

### 图表特性
- 📊 基于 Ant Design Charts 构建
- 🔍 支持数据缩放和时间范围选择
- 📱 响应式设计，支持移动端
- 🎨 美观的工具提示和图例
- ⚡ 流畅的动画效果

### 下一步计划
- [x] 集成股票蜡烛图和MA均线
- [ ] 添加成交量柱状图
- [ ] 实现MACD、RSI等技术指标图表
- [ ] 添加更多股票市场支持
- [ ] 实现用户偏好设置
- [ ] 添加数据缓存机制