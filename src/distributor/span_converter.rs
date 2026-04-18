use storage::span::Span;

pub trait UnifiedSpanConverter {
    
    fn into_unified(self) -> Vec<Span>;
    
}