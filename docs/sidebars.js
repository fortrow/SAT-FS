// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'intro',
    'current-goals',
    {
      type: 'category',
      label: 'Trainer Surface',
      items: ['trainer/acceptance-fixture', 'trainer/python-streaming'],
    },
    {
      type: 'category',
      label: 'Transport',
      items: ['transport/quic'],
    },
    {
      type: 'category',
      label: 'Operations',
      items: ['ops/deployment'],
    },
    {
      type: 'category',
      label: 'Linux Serving Host',
      items: ['blockfs/cxbfs'],
    },
  ],
};

module.exports = sidebars;
