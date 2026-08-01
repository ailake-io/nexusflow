import { useAuth } from '@/lib/auth'
import { LoginForm } from '@/components/LoginForm'
import { DagCanvas } from '@/components/DagCanvas'

function App() {
  const { token } = useAuth()
  return token ? <DagCanvas /> : <LoginForm />
}

export default App
