package sync_tool

type BatchContext interface {
	AddSerialTask(task func())
}
