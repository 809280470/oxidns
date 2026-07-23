const sidebars = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: '入门',
      collapsed: false,
      items: [
        'quickstart',
        {
          type: 'category',
          label: '安装与部署',
          items: [
            'installation/native',
            'installation/packages',
            'installation/docker',
            'openwrt',
          ],
        },
        'scenarios',
        'migrate-from-mosdns',
      ],
    },
    {
      type: 'category',
      label: '配置指南',
      items: ['configuration'],
    },
    {
      type: 'category',
      label: '插件参考',
      items: [
        'plugin-reference/overview',
        'plugin-reference/server',
        'plugin-reference/executor',
        'plugin-reference/matcher',
        'plugin-reference/provider',
      ],
    },
    {
      type: 'category',
      label: '部署与运维',
      items: ['webui', 'operations', 'security'],
    },
    {
      type: 'category',
      label: '接口参考',
      items: ['cli', 'api', 'dns-codes'],
    },
    {
      type: 'category',
      label: '架构与开发',
      items: ['architecture-and-design', 'custom-build', 'benchmarks'],
    },
    {
      type: 'category',
      label: '项目与社区',
      items: ['contributing', 'roadmap', 'releases'],
    },
  ],
};

export default sidebars;
