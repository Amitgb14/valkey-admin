import { type FormEvent, useState, useEffect } from "react"
import { Loader2, X } from "lucide-react"
import * as Dialog from "@radix-ui/react-dialog"
import { Alert, AlertDescription } from "./alert.tsx"
import { Button } from "./button.tsx"
import { Input } from "./input.tsx"
import { Typography } from "./typography.tsx"
import { Label } from "./label.tsx"

interface PasswordPromptModalProps {
  open: boolean
  onClose: () => void
  onSubmit: (password: string) => void
  isConnecting?: boolean
  errorMessage?: string
  connectionLabel: string
}

export function PasswordPromptModal({
  open,
  onClose,
  onSubmit,
  isConnecting = false,
  errorMessage,
  connectionLabel,
}: PasswordPromptModalProps) {
  const [password, setPassword] = useState("")

  // Clear password when error changes (wrong password)
  useEffect(() => {
    if (errorMessage) setPassword("")
  }, [errorMessage])

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault()
    onSubmit(password)
  }

  const handleClose = () => {
    setPassword("")
    onClose()
  }

  return (
    <Dialog.Root onOpenChange={handleClose} open={open}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-30 bg-black/50" />
        <Dialog.Content asChild>
          <div className="fixed inset-0 z-40 flex items-center justify-center">
            <div className="w-full max-w-sm p-6 bg-white dark:bg-tw-dark-primary dark:border-tw-dark-border rounded-lg shadow-lg border">
              <div className="flex justify-between">
                <Dialog.Title asChild>
                  <Typography variant="subheading">Password Required</Typography>
                </Dialog.Title>
                <Dialog.Close asChild>
                  <Button className="hover:text-primary h-auto p-0" variant="ghost">
                    <X size={20} />
                  </Button>
                </Dialog.Close>
              </div>
              <Dialog.Description asChild>
                <Typography variant="bodySm">
                  Enter password for <strong>{connectionLabel}</strong>
                </Typography>
              </Dialog.Description>

              {errorMessage && (
                <Alert className="mt-4" variant="destructive">
                  <AlertDescription>{errorMessage}</AlertDescription>
                </Alert>
              )}

              <form className="space-y-4 mt-4" onSubmit={handleSubmit}>
                <div>
                  <Label className="block mb-1" htmlFor="prompt-password">
                    Password
                  </Label>
                  <Input
                    autoFocus
                    id="prompt-password"
                    onChange={(e) => setPassword(e.target.value)}
                    type="password"
                    value={password}
                  />
                </div>

                <div className="pt-2 text-sm">
                  <Button
                    className="w-full"
                    disabled={isConnecting}
                    type="submit"
                  >
                    {isConnecting && <Loader2 className="animate-spin" size={16} />}
                    {isConnecting ? "Connecting..." : "Connect"}
                  </Button>
                </div>
              </form>
            </div>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
