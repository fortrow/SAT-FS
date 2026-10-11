/// <reference types="vite/client" />

type CordshockPlatformInfo = {
  platform: string
  versions: {
    chrome: string
    electron: string
    node: string
  }
}

type SatfsTransferInfo = {
  name: string
  route: string
  state: string
  throughput: string
  compression: string
}

type SatfsProbeResult = {
  reachable: boolean
  latencyMs: number
  status: string
  message: string
}

type SatfsMountResult = {
  success: boolean
  mountPoint: string
  message: string
}

type SatfsMetrics = {
  throughputMBps: number
  activeCords: number
  compressionRatio: number
  totalBytesTransferred: number
}

interface SatfsApi {
  getPlatform: () => Promise<CordshockPlatformInfo>
  probeEndpoint: (params: {
    host: string
    controlPort: number
    bulkPort: number
    secureMode: boolean
    namespace: string
  }) => Promise<SatfsProbeResult>
  getTransfers: () => Promise<SatfsTransferInfo[]>
  mount: (params: {
    host: string
    controlPort: number
    remotePath: string
    mountPoint: string
  }) => Promise<SatfsMountResult>
  unmount: (mountPoint: string) => Promise<{ success: boolean; message: string }>
  getMetrics: () => Promise<SatfsMetrics>
}

interface Window {
  satfs: SatfsApi
  cordshock: SatfsApi
}
