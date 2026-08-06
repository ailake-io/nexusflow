interface LogoProps {
  className?: string
  showText?: boolean
}

export function Logo({ className = '', showText = true }: LogoProps) {
  return (
    <div className={`inline-flex items-center gap-2.5 ${className}`}>
      <svg
        width="28"
        height="28"
        viewBox="0 0 32 32"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className="shrink-0"
      >
        <rect width="32" height="32" rx="7" className="fill-primary/10 stroke-primary/30" strokeWidth="1" />
        <path
          d="M8 20c0-4 4-8 8-8s8 4 8 8"
          className="stroke-primary"
          strokeWidth="2.5"
          strokeLinecap="round"
        />
        <circle cx="16" cy="13" r="2.5" className="fill-primary" />
        <circle cx="10" cy="21" r="2" className="fill-accent" />
        <circle cx="22" cy="21" r="2" className="fill-accent" />
        <path d="M16 15.5v3" className="stroke-primary" strokeWidth="2" strokeLinecap="round" />
      </svg>
      {showText && (
        <span className="text-lg font-semibold tracking-tight text-foreground">
          Nexus<span className="text-primary">Flow</span>
        </span>
      )}
    </div>
  )
}
