import { contextBridge, ipcRenderer } from 'electron'

const api = {
  getPlatform: () => ipcRenderer.invoke('satfs:getPlatform'),
  probeEndpoint: (params: {
    host: string
    controlPort: number
    bulkPort: number
    secureMode: boolean
    namespace: string
  }) => ipcRenderer.invoke('satfs:probeEndpoint', params),
  getTransfers: () => ipcRenderer.invoke('satfs:getTransfers'),
  mount: (params: {
    host: string
    controlPort: number
    remotePath: string
    mountPoint: string
  }) => ipcRenderer.invoke('satfs:mount', params),
  unmount: (mountPoint: string) => ipcRenderer.invoke('satfs:unmount', mountPoint),
  getMetrics: () => ipcRenderer.invoke('satfs:getMetrics')
}

contextBridge.exposeInMainWorld('satfs', api)
contextBridge.exposeInMainWorld('cordshock', api)

export type SatfsApi = typeof api
