import { ValidationResult } from '@aurelia/validation';
import { ValidationEvent, ValidationResultsSubscriber } from '../validation-controller';
export interface IValidationResultPresenterService extends ValidationResultPresenterService {
}
export declare const IValidationResultPresenterService: import("@aurelia/kernel").InterfaceSymbol<IValidationResultPresenterService>;
export declare class ValidationResultPresenterService implements ValidationResultsSubscriber {
    private readonly platform;
    handleValidationEvent(event: ValidationEvent): void;
    remove(target: Element, results: ValidationResult[]): void;
    add(target: Element, results: ValidationResult[]): void;
    getValidationMessageContainer(target: Element): Element | null;
    showResults(messageContainer: Element, results: ValidationResult[]): void;
    removeResults(messageContainer: Element, results: ValidationResult[]): void;
    private reverseMap;
}
//# sourceMappingURL=validation-result-presenter-service.d.ts.map