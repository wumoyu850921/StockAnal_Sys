import { defineConfig } from 'umi';

export default defineConfig({
  routes: [
    {
      path: '/',
      redirect: '/home',
    },
    {
      path: '/home',
      component: '@/pages/Home',
    },
    {
      path: '/market-scan',
      component: '@/pages/MarketScan',
    },
    {
      path: '/stock/:code',
      component: '@/pages/Stock/Detail',
    },
    {
      path: '/capital-flow',
      component: '@/pages/CapitalFlow',
    },
    {
      path: '/data-sync',
      component: '@/pages/DataSync',
    },
    {
      path: '/kline',
      component: '@/pages/KLine',
    },
  ],
  npmClient: 'npm',
  title: '股票分析系统',
  hash: true,
  history: {
    type: 'hash',
  },
  outputPath: 'dist',
  alias: {
    '@': './src',
  },
});