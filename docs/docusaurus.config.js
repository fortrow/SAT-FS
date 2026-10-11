// @ts-check

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'CX Project Docs',
  tagline: 'Concurrent transfer storage for network datasets and Linux serving hosts',
  url: 'https://fortrow.github.io',
  baseUrl: '/cxnfs/',
  organizationName: 'fortrow',
  projectName: 'cxnfs',
  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
  themeConfig: {
    navbar: {
      title: 'CX',
      items: [
        {to: '/', label: 'Overview', position: 'left'},
        {to: '/trainer/acceptance-fixture', label: 'Trainer Surface', position: 'left'},
        {to: '/transport/quic', label: 'QUIC', position: 'left'},
        {to: '/blockfs/cxbfs', label: 'CXBFS', position: 'left'},
      ],
    },
    prism: {
      additionalLanguages: ['bash', 'java', 'python'],
    },
  },
};

module.exports = config;
