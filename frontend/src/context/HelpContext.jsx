import { createContext, useContext, useState, useCallback } from 'react'

const HelpContext = createContext(null)

export function HelpProvider({ children }) {
  const [isHelpOpen, setIsHelpOpen] = useState(false)
  const [activeSection, setActiveSection] = useState(null)
  const [searchTerm, setSearchTerm] = useState('')
  const [highlightedTerm, setHighlightedTerm] = useState(null)

  const openHelp = useCallback((sectionId = null) => {
    setIsHelpOpen(true)
    if (sectionId) setActiveSection(sectionId)
  }, [])

  const closeHelp = useCallback(() => {
    setIsHelpOpen(false)
    setActiveSection(null)
    setHighlightedTerm(null)
  }, [])

  const toggleHelp = useCallback(() => {
    setIsHelpOpen((prev) => {
      if (prev) {
        setActiveSection(null)
        setHighlightedTerm(null)
      }
      return !prev
    })
  }, [])

  return (
    <HelpContext.Provider
      value={{
        isHelpOpen,
        activeSection,
        setActiveSection,
        searchTerm,
        setSearchTerm,
        highlightedTerm,
        setHighlightedTerm,
        openHelp,
        closeHelp,
        toggleHelp,
      }}
    >
      {children}
    </HelpContext.Provider>
  )
}

export const useHelp = () => useContext(HelpContext)
